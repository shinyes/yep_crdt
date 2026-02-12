// 示例：使用 sync 模块
package main

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/shinyes/yep_crdt/pkg/db"
	"github.com/shinyes/yep_crdt/pkg/meta"
	"github.com/shinyes/yep_crdt/pkg/store"
	"github.com/shinyes/yep_crdt/pkg/sync"
)

func main() {
	// 1. 初始化数据库
	dbPath := "./data/sync_example"
	os.MkdirAll(dbPath, 0755)

	s, err := store.NewBadgerStore(dbPath)
	if err != nil {
		log.Fatal(err)
	}
	defer s.Close()

	database := db.Open(s, "sync-example")
	database.SetFileStorageDir("./data/files")

	// 2. 定义表
	err = database.DefineTable(&meta.TableSchema{
		Name: "users",
		Columns: []meta.ColumnSchema{
			{Name: "name", Type: meta.ColTypeString, CrdtType: meta.CrdtLWW},
			{Name: "age", Type: meta.ColTypeInt, CrdtType: meta.CrdtLWW},
			{Name: "tags", Type: meta.ColTypeString, CrdtType: meta.CrdtORSet},
		},
	})
	if err != nil {
		log.Fatal(err)
	}

	table := database.Table("users")

	// 3. 创建节点管理器
	nodeManager := sync.NewNodeManager(database, "node-1",
		sync.WithHeartbeatInterval(5*time.Second),
		sync.WithTimeoutThreshold(30*time.Second),
		sync.WithClockThreshold(5000), // 5 秒
		sync.WithGCInterval(1*time.Minute),
		sync.WithGCTimeOffset(30*time.Second),
	)

	// 4. 启动
	ctx := context.Background()
	nodeManager.Start(ctx)

	log.Println("✅ 节点管理器已启动")
	log.Printf("   本地节点: %s\n", nodeManager.GetLocalNodeID())
	log.Printf("   心跳间隔: 5s\n")
	log.Printf("   超时阈值: 30s\n")
	log.Printf("   时钟阈值: 5000\n")
	log.Printf("   GC 间隔: 1m\n")
	log.Printf("   GC 偏移: 30s\n")

	// 5. 模拟其他节点的心跳
	go func() {
		time.Sleep(2 * time.Second)
		
		// 模拟节点 2 的心跳
		nodeID2 := "node-2"
		clock2 := int64(1000)
		nodeManager.OnHeartbeat(nodeID2, clock2)
		
		log.Printf("📤 模拟节点 %s 的心跳，时钟: %d\n", nodeID2, clock2)
		
		// 持续发送心跳
		ticker := time.NewTicker(5 * time.Second)
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				clock2 += 100 // 时钟递增
				nodeManager.OnHeartbeat(nodeID2, clock2)
			}
		}
	}()

	log.Println("✅ 程序已启动")
	log.Println("   按 Ctrl+C 停止")

	// 6. 主循环
	select {}
}
