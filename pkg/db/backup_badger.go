package db

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/shinyes/yep_crdt/pkg/meta"
	"github.com/shinyes/yep_crdt/pkg/store"
)

const defaultRestoreMaxPendingWrites = 256

type badgerBackupLoader interface {
	Backup(w io.Writer, since uint64) (uint64, error)
	Load(r io.Reader, maxPendingWrites int) error
}

// BackupToLocal exports current Badger data to one local file.
// This backup only contains Badger key/value data, and does not include FileStorageDir files.
func (db *DB) BackupToLocal(path string) (uint64, error) {
	return db.BackupToLocalSince(path, 0)
}

// BackupToLocalSince exports current Badger data to one local file.
// since=0 means full backup, otherwise it is incremental since the returned version.
// This backup only contains Badger key/value data, and does not include FileStorageDir files.
func (db *DB) BackupToLocalSince(path string, since uint64) (uint64, error) {
	if db == nil || db.store == nil {
		return 0, fmt.Errorf("db is nil")
	}

	backupPath := strings.TrimSpace(path)
	if backupPath == "" {
		return 0, fmt.Errorf("backup path cannot be empty")
	}

	loader, ok := db.store.(badgerBackupLoader)
	if !ok {
		return 0, fmt.Errorf("store does not support badger backup/load: %T", db.store)
	}

	if err := os.MkdirAll(filepath.Dir(backupPath), 0o755); err != nil {
		return 0, err
	}

	tmpPath := backupPath + ".tmp"
	tmpFile, err := os.Create(tmpPath)
	if err != nil {
		return 0, err
	}

	cleanupTmp := true
	defer func() {
		_ = tmpFile.Close()
		if cleanupTmp {
			_ = os.Remove(tmpPath)
		}
	}()

	sinceOut, err := loader.Backup(tmpFile, since)
	if err != nil {
		return 0, err
	}
	if err := tmpFile.Sync(); err != nil {
		return 0, err
	}
	if err := tmpFile.Close(); err != nil {
		return 0, err
	}

	if err := replaceFileWithBackup(backupPath, tmpPath); err != nil {
		return 0, err
	}
	cleanupTmp = false

	return sinceOut, nil
}

func replaceFileWithBackup(destPath string, tmpPath string) error {
	backupPath := destPath + ".old"
	hasOriginal := false

	if _, err := os.Stat(destPath); err == nil {
		_ = os.Remove(backupPath)
		if err := os.Rename(destPath, backupPath); err != nil {
			return err
		}
		hasOriginal = true
	} else if !os.IsNotExist(err) {
		return err
	}

	if err := os.Rename(tmpPath, destPath); err != nil {
		if hasOriginal {
			_ = os.Rename(backupPath, destPath)
		}
		return err
	}
	if hasOriginal {
		_ = os.Remove(backupPath)
	}
	return nil
}

// BadgerRestoreConfig defines restoring one Badger database from local backup.
type BadgerRestoreConfig struct {
	BackupPath             string
	Path                   string
	DatabaseID             string
	BadgerValueLogFileSize int64 // 0 means store default.
	BadgerOptions          []store.BadgerOption
	DBOptions              []Option
	Schemas                []*meta.TableSchema
	EnsureSchema           func(*DB) error
	MaxPendingWrites       int  // <=0 uses defaultRestoreMaxPendingWrites.
	ReplaceExisting        bool // true means delete target db directory before restore.
}

// RestoreBadgerFromLocalBackup restores one Badger backup file to cfg.Path.
// Data will be created under the parent directory of cfg.Path.
// This restore only covers Badger key/value data, and does not restore FileStorageDir files.
func RestoreBadgerFromLocalBackup(cfg BadgerRestoreConfig) (*DB, error) {
	path := strings.TrimSpace(cfg.Path)
	if path == "" {
		return nil, fmt.Errorf("badger path cannot be empty")
	}
	databaseID := strings.TrimSpace(cfg.DatabaseID)
	if databaseID == "" {
		return nil, fmt.Errorf("database id cannot be empty")
	}
	backupPath := strings.TrimSpace(cfg.BackupPath)
	if backupPath == "" {
		return nil, fmt.Errorf("backup path cannot be empty")
	}
	if cfg.BadgerValueLogFileSize < 0 {
		return nil, fmt.Errorf("badger value log file size must be >= 0, got %d", cfg.BadgerValueLogFileSize)
	}
	if cfg.BadgerValueLogFileSize > 0 &&
		(cfg.BadgerValueLogFileSize < minBadgerValueLogFileSize ||
			cfg.BadgerValueLogFileSize >= maxBadgerValueLogFileSize) {
		return nil, fmt.Errorf(
			"badger value log file size must be 0 or in range [%d, %d), got %d",
			minBadgerValueLogFileSize,
			maxBadgerValueLogFileSize,
			cfg.BadgerValueLogFileSize,
		)
	}
	if cfg.MaxPendingWrites < 0 {
		return nil, fmt.Errorf("max pending writes must be >= 0, got %d", cfg.MaxPendingWrites)
	}

	parentDir := filepath.Dir(path)
	if err := os.MkdirAll(parentDir, 0o755); err != nil {
		return nil, err
	}

	if cfg.ReplaceExisting {
		if err := os.RemoveAll(path); err != nil {
			return nil, err
		}
	} else {
		entries, err := os.ReadDir(path)
		if err != nil && !os.IsNotExist(err) {
			return nil, err
		}
		if len(entries) > 0 {
			return nil, fmt.Errorf("target db path is not empty: %s", path)
		}
	}

	if err := os.MkdirAll(path, 0o755); err != nil {
		return nil, err
	}

	badgerOptions := make([]store.BadgerOption, 0, len(cfg.BadgerOptions)+1)
	badgerOptions = append(badgerOptions, cfg.BadgerOptions...)
	if cfg.BadgerValueLogFileSize > 0 {
		badgerOptions = append(badgerOptions, store.WithBadgerValueLogFileSize(cfg.BadgerValueLogFileSize))
	}

	kv, err := store.NewBadgerStore(path, badgerOptions...)
	if err != nil {
		return nil, err
	}

	loader, ok := any(kv).(badgerBackupLoader)
	if !ok {
		_ = kv.Close()
		return nil, fmt.Errorf("store does not support badger backup/load: %T", kv)
	}

	backupFile, err := os.Open(backupPath)
	if err != nil {
		_ = kv.Close()
		return nil, err
	}

	maxPendingWrites := cfg.MaxPendingWrites
	if maxPendingWrites <= 0 {
		maxPendingWrites = defaultRestoreMaxPendingWrites
	}
	if err := loader.Load(backupFile, maxPendingWrites); err != nil {
		_ = backupFile.Close()
		_ = kv.Close()
		return nil, err
	}
	if err := backupFile.Close(); err != nil {
		_ = kv.Close()
		return nil, err
	}
	if err := kv.Close(); err != nil {
		return nil, err
	}

	return OpenBadgerWithConfig(BadgerOpenConfig{
		Path:                   path,
		DatabaseID:             databaseID,
		BadgerValueLogFileSize: cfg.BadgerValueLogFileSize,
		BadgerOptions:          cfg.BadgerOptions,
		DBOptions:              cfg.DBOptions,
		Schemas:                cfg.Schemas,
		EnsureSchema:           cfg.EnsureSchema,
	})
}
