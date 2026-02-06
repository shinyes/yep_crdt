package store

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
)

// Fetcher 定义了如何从外部源检索缺失的 blob。
// 此接口允许 blob 存储是 "主动" 的 - 尝试修复缺失的数据。
type Fetcher interface {
	Fetch(hash string) ([]byte, error)
}

// DiskBlobStore 使用本地文件系统实现 BlobStore。
type DiskBlobStore struct {
	baseDir string
	fetcher Fetcher
}

func NewDiskBlobStore(baseDir string) (*DiskBlobStore, error) {
	if err := os.MkdirAll(baseDir, 0755); err != nil {
		return nil, err
	}
	return &DiskBlobStore{baseDir: baseDir}, nil
}

func (bs *DiskBlobStore) SetFetcher(f Fetcher) {
	bs.fetcher = f
}

func (bs *DiskBlobStore) hashToPath(hash string) string {
	// 使用 2 级目录分片（例如 ab/cd/abcdef...）以避免一个目录中文件过多
	if len(hash) < 4 {
		return filepath.Join(bs.baseDir, hash)
	}
	return filepath.Join(bs.baseDir, hash[:2], hash[2:4], hash)
}

func (bs *DiskBlobStore) Put(data []byte) (string, error) {
	sum := sha256.Sum256(data)
	hash := hex.EncodeToString(sum[:])

	path := bs.hashToPath(hash)

	// 检查是否已存在
	if _, err := os.Stat(path); err == nil {
		return hash, nil
	}

	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return "", err
	}

	if err := os.WriteFile(path, data, 0644); err != nil {
		return "", err
	}

	return hash, nil
}

func (bs *DiskBlobStore) Get(hash string) ([]byte, error) {
	path := bs.hashToPath(hash)

	data, err := os.ReadFile(path)
	if err == nil {
		return data, nil
	}

	// 如果本地读取失败，尝试获取
	if os.IsNotExist(err) && bs.fetcher != nil {
		fmt.Printf("Blob %s 缺失，正在从网络获取...\n", hash)
		data, err := bs.fetcher.Fetch(hash)
		if err != nil {
			return nil, err
		}
		// 保存到本地存储
		// 校验哈希？
		sum := sha256.Sum256(data)
		if hex.EncodeToString(sum[:]) != hash {
			return nil, fmt.Errorf("获取的数据哈希不匹配")
		}

		if _, err := bs.Put(data); err != nil {
			// 警告但仍返回数据？
			fmt.Printf("警告：保存获取的 blob 失败：%v\n", err)
		}
		return data, nil
	}

	return nil, err
}

func (bs *DiskBlobStore) Has(hash string) (bool, error) {
	path := bs.hashToPath(hash)
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}
