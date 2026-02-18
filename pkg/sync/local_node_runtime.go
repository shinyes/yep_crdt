package sync

import (
	"archive/zip"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/shinyes/yep_crdt/pkg/db"
)

// Close stops sync and closes all opened databases.
func (n *LocalNode) Close() error {
	n.mu.Lock()
	if n.closed {
		n.mu.Unlock()
		return nil
	}
	n.closed = true
	engine := n.engine
	databases := make([]*db.DB, 0, len(n.databases))
	for _, database := range n.databases {
		databases = append(databases, database)
	}
	n.mu.Unlock()

	if engine != nil {
		engine.Stop()
	}

	var errs []error
	for _, database := range databases {
		if database == nil {
			continue
		}
		if err := database.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	return errors.Join(errs...)
}

// Engine returns the running multi-tenant engine.
func (n *LocalNode) Engine() *MultiEngine {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.engine
}

// ManualGCTenant triggers negotiated manual GC for one tenant.
func (n *LocalNode) ManualGCTenant(tenantID string, timeout time.Duration) (*ManualGCResult, error) {
	n.mu.RLock()
	engine := n.engine
	closed := n.closed
	n.mu.RUnlock()

	if closed {
		return nil, fmt.Errorf("local node is closed")
	}
	if engine == nil {
		return nil, fmt.Errorf("sync engine is not started")
	}

	return engine.ManualGC(tenantID, timeout)
}

// BackupTenant exports one tenant Badger database into a local backup file.
// It only backs up Badger KV data and does not include FileStorageDir files.
func (n *LocalNode) BackupTenant(tenantID string, backupPath string) (uint64, error) {
	return n.BackupTenantSince(tenantID, backupPath, 0)
}

// BackupTenantSince exports one tenant Badger database into a local backup file.
// since=0 means full backup.
func (n *LocalNode) BackupTenantSince(tenantID string, backupPath string, since uint64) (uint64, error) {
	tenantID = strings.TrimSpace(tenantID)
	if tenantID == "" {
		return 0, fmt.Errorf("tenant id cannot be empty")
	}
	backupPath = strings.TrimSpace(backupPath)
	if backupPath == "" {
		return 0, fmt.Errorf("backup path cannot be empty")
	}

	n.mu.RLock()
	closed := n.closed
	database, exists := n.databases[tenantID]
	n.mu.RUnlock()

	if closed {
		return 0, fmt.Errorf("local node is closed")
	}
	if !exists || database == nil {
		return 0, fmt.Errorf("tenant not started: %s", tenantID)
	}

	return database.BackupToLocalSince(backupPath, since)
}

type localNodeArchiveTenant struct {
	TenantID string `json:"tenant_id"`
	File     string `json:"file"`
	Since    uint64 `json:"since"`
}

type localNodeArchiveManifest struct {
	Version int                      `json:"version"`
	Tenants []localNodeArchiveTenant `json:"tenants"`
}

// BackupAllTenants exports all running tenant Badger databases into one local archive file.
// The archive only contains Badger KV data and does not include FileStorageDir files.
func (n *LocalNode) BackupAllTenants(archivePath string) (map[string]uint64, error) {
	archivePath = strings.TrimSpace(archivePath)
	if archivePath == "" {
		return nil, fmt.Errorf("archive path cannot be empty")
	}

	n.mu.RLock()
	closed := n.closed
	tenantDatabases := make(map[string]*db.DB, len(n.databases))
	for tenantID, database := range n.databases {
		if database != nil {
			tenantDatabases[tenantID] = database
		}
	}
	n.mu.RUnlock()

	if closed {
		return nil, fmt.Errorf("local node is closed")
	}
	if len(tenantDatabases) == 0 {
		return nil, fmt.Errorf("no tenant started")
	}

	if err := os.MkdirAll(filepath.Dir(archivePath), 0o755); err != nil {
		return nil, err
	}

	tmpDir, err := os.MkdirTemp("", "yep_crdt_backup_all_")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(tmpDir)

	tmpArchivePath := archivePath + ".tmp"
	archiveFile, err := os.Create(tmpArchivePath)
	if err != nil {
		return nil, err
	}

	cleanupArchive := true
	defer func() {
		_ = archiveFile.Close()
		if cleanupArchive {
			_ = os.Remove(tmpArchivePath)
		}
	}()

	zipWriter := zip.NewWriter(archiveFile)
	shouldCloseZip := true
	defer func() {
		if shouldCloseZip {
			_ = zipWriter.Close()
		}
	}()

	tenantIDs := make([]string, 0, len(tenantDatabases))
	for tenantID := range tenantDatabases {
		tenantIDs = append(tenantIDs, tenantID)
	}
	sort.Strings(tenantIDs)

	sinceByTenant := make(map[string]uint64, len(tenantIDs))
	manifest := localNodeArchiveManifest{
		Version: 1,
		Tenants: make([]localNodeArchiveTenant, 0, len(tenantIDs)),
	}

	for _, tenantID := range tenantIDs {
		database := tenantDatabases[tenantID]

		tenantBackupPath := filepath.Join(tmpDir, hex.EncodeToString([]byte(tenantID))+".badgerbak")
		since, err := database.BackupToLocal(tenantBackupPath)
		if err != nil {
			return nil, err
		}

		archiveEntry := "tenants/" + hex.EncodeToString([]byte(tenantID)) + ".badgerbak"
		if err := copyFileToZip(zipWriter, archiveEntry, tenantBackupPath); err != nil {
			return nil, err
		}

		sinceByTenant[tenantID] = since
		manifest.Tenants = append(manifest.Tenants, localNodeArchiveTenant{
			TenantID: tenantID,
			File:     archiveEntry,
			Since:    since,
		})
	}

	manifestBytes, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return nil, err
	}
	manifestWriter, err := zipWriter.Create("manifest.json")
	if err != nil {
		return nil, err
	}
	if _, err := manifestWriter.Write(manifestBytes); err != nil {
		return nil, err
	}

	if err := zipWriter.Close(); err != nil {
		return nil, err
	}
	shouldCloseZip = false

	if err := archiveFile.Sync(); err != nil {
		return nil, err
	}
	if err := archiveFile.Close(); err != nil {
		return nil, err
	}

	_ = os.Remove(archivePath)
	if err := os.Rename(tmpArchivePath, archivePath); err != nil {
		return nil, err
	}
	cleanupArchive = false

	return sinceByTenant, nil
}

func copyFileToZip(zipWriter *zip.Writer, archiveEntry string, srcPath string) error {
	srcFile, err := os.Open(srcPath)
	if err != nil {
		return err
	}
	defer srcFile.Close()

	dstWriter, err := zipWriter.Create(archiveEntry)
	if err != nil {
		return err
	}
	_, err = io.Copy(dstWriter, srcFile)
	return err
}

// RestoreTenant restores one tenant backup into local data root.
// Restored data path is "<dataRoot>/<tenantID>".
// It only restores Badger KV data and does not restore FileStorageDir files.
func (n *LocalNode) RestoreTenant(opts TenantRestoreOptions) error {
	tenantID := strings.TrimSpace(opts.TenantID)
	if tenantID == "" {
		return fmt.Errorf("tenant id cannot be empty")
	}
	backupPath := strings.TrimSpace(opts.BackupPath)
	if backupPath == "" {
		return fmt.Errorf("backup path cannot be empty")
	}

	n.mu.RLock()
	closed := n.closed
	_, tenantRunning := n.databases[tenantID]
	dataRoot := n.dataRoot
	defaultVlog := n.badgerValueLogFileSize
	defaultEnsureSchema := n.ensureSchema
	n.mu.RUnlock()

	if !closed && tenantRunning {
		return fmt.Errorf("tenant is running in current local node, close node before restore: %s", tenantID)
	}

	if dataRoot == "" {
		dataRoot = "."
	}
	targetPath := filepath.Join(dataRoot, tenantID)

	vlogSize := opts.BadgerValueLogFileSize
	if vlogSize == 0 {
		vlogSize = defaultVlog
	}
	ensureSchema := opts.EnsureSchema
	if ensureSchema == nil {
		ensureSchema = defaultEnsureSchema
	}

	restoredDB, err := db.RestoreBadgerFromLocalBackup(db.BadgerRestoreConfig{
		BackupPath:             backupPath,
		Path:                   targetPath,
		DatabaseID:             tenantID,
		BadgerValueLogFileSize: vlogSize,
		EnsureSchema:           ensureSchema,
		MaxPendingWrites:       opts.MaxPendingWrites,
		ReplaceExisting:        opts.ReplaceExisting,
	})
	if err != nil {
		return err
	}
	return restoredDB.Close()
}

// TenantDatabase returns one opened tenant database and whether it exists.
func (n *LocalNode) TenantDatabase(tenantID string) (*db.DB, bool) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	database, ok := n.databases[tenantID]
	if !ok || database == nil {
		return nil, false
	}
	return database, true
}

// TenantIDs returns all started tenant IDs.
func (n *LocalNode) TenantIDs() []string {
	n.mu.RLock()
	defer n.mu.RUnlock()

	out := make([]string, len(n.tenantIDs))
	copy(out, n.tenantIDs)
	return out
}
