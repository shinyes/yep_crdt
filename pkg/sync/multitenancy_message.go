package sync

import stdlog "log"

// handleMessage 处理收到的消息
func (m *MultiTenantManager) handleMessage(tenantID, peerID string, msg NetworkMessage) {
	tnm, exists := m.tenants[tenantID]
	if !exists {
		stdlog.Printf("[MultiTenantManager] 收到未知租户的消息: %s", tenantID)
		return
	}

	switch msg.Type {
	case MsgTypeHeartbeat:
		// 更新节点心跳
		tnm.nodeMgr.OnHeartbeat(peerID, msg.Clock, msg.GCFloor)
		// 更新本地时钟
		if msg.Clock > 0 {
			tnm.nodeMgr.UpdateLocalClock(msg.Clock)
		}

	case MsgTypeLocalFileChunk:
		if msg.RequestID == "" && !tnm.nodeMgr.CanUseIncrementalWithPeer(peerID) {
			stdlog.Printf("[MultiTenantManager:%s] skip local file chunk from blocked incremental peer: from=%s", tenantID, shortPeerID(peerID))
			return
		}
		if tnm.chunks == nil {
			stdlog.Printf("[MultiTenantManager:%s] local file chunk receiver not initialized", tenantID)
			return
		}
		if err := tnm.chunks.HandleChunk(peerID, tnm.db.FileStorageDir, msg); err != nil {
			stdlog.Printf("[MultiTenantManager:%s] apply local file chunk failed: path=%s, idx=%d/%d, from=%s, err=%v",
				tenantID, msg.FilePath, msg.ChunkIndex+1, msg.ChunkTotal, shortPeerID(peerID), err)
		}

	case MsgTypeRawData:
		if msg.GCFloor > 0 {
			tnm.nodeMgr.ObservePeerGCFloor(peerID, msg.GCFloor)
		}
		if !tnm.nodeMgr.CanUseIncrementalWithPeer(peerID) {
			stdlog.Printf("[MultiTenantManager:%s] skip raw data from blocked incremental peer: from=%s", tenantID, shortPeerID(peerID))
			return
		}
		// 处理原始 CRDT 字节同步
		if msg.Table != "" && msg.Key != "" && msg.RawData != nil {
			stdlog.Printf("[MultiTenantManager:%s] 收到原始数据: table=%s, key=%s, from=%s", tenantID, msg.Table, msg.Key, shortPeerID(peerID))
			err := tnm.nodeMgr.dataSync.OnReceiveMergeWithFiles(msg.Table, msg.Key, msg.RawData, msg.Timestamp, msg.LocalFiles)
			if err != nil {
				stdlog.Printf("[MultiTenantManager:%s] Merge 数据失败: %v", tenantID, err)
			} else {
				stdlog.Printf("[MultiTenantManager:%s] Merge 数据成功", tenantID)
			}
		}

	case MsgTypeRawDelta:
		if msg.GCFloor > 0 {
			tnm.nodeMgr.ObservePeerGCFloor(peerID, msg.GCFloor)
		}
		if !tnm.nodeMgr.CanUseIncrementalWithPeer(peerID) {
			stdlog.Printf("[MultiTenantManager:%s] skip raw delta from blocked incremental peer: from=%s", tenantID, shortPeerID(peerID))
			return
		}
		if msg.Table != "" && msg.Key != "" && msg.RawData != nil {
			stdlog.Printf("[MultiTenantManager:%s] received delta data: table=%s, key=%s, cols=%v, from=%s",
				tenantID, msg.Table, msg.Key, msg.Columns, shortPeerID(peerID))
			if err := tnm.nodeMgr.dataSync.OnReceiveDeltaWithFiles(msg.Table, msg.Key, msg.Columns, msg.RawData, msg.Timestamp, msg.LocalFiles); err != nil {
				stdlog.Printf("[MultiTenantManager:%s] merge delta failed: %v", tenantID, err)
			}
		}

	case MsgTypeFetchRawRequest:
		// 处理原始数据获取请求
		m.handleFetchRawRequest(tnm, peerID, msg)

	case MsgTypeFetchRawResponse:
		// 响应已由 TenantNetwork 请求等待器处理

	case MsgTypeGCPrepare:
		tnm.nodeMgr.HandleManualGCPrepare(peerID, msg)

	case MsgTypeGCCommit:
		tnm.nodeMgr.HandleManualGCCommit(peerID, msg)

	case MsgTypeGCExecute:
		tnm.nodeMgr.HandleManualGCExecute(peerID, msg)

	case MsgTypeGCAbort:
		tnm.nodeMgr.HandleManualGCAbort(peerID, msg)
	}
}

// handleFetchRawRequest 处理原始数据获取请求
func (m *MultiTenantManager) handleFetchRawRequest(tnm *TenantNodeManager, peerID string, msg NetworkMessage) {
	if msg.Table == "" {
		return
	}

	// 使用 DataSyncManager 导出表的原始数据
	rawRows, err := tnm.nodeMgr.dataSync.ExportTableRawData(msg.Table)
	if err != nil {
		stdlog.Printf("[MultiTenantManager:%s] 导出表数据失败: %v", tnm.tenantID, err)
		return
	}

	// 逐行发送原始数据（用响应消息格式）
	for _, row := range rawRows {
		if err := tnm.nodeMgr.dataSync.sendChunkedLocalFilesToPeer(peerID, msg.Table, row.Key, msg.RequestID, row.LocalFiles); err != nil {
			stdlog.Printf("[MultiTenantManager:%s] 发送文件分块失败: table=%s, key=%s, err=%v",
				tnm.tenantID, msg.Table, shortPeerID(row.Key), err)
			continue
		}

		responseMsg := &NetworkMessage{
			Type:       MsgTypeFetchRawResponse,
			RequestID:  msg.RequestID,
			Table:      msg.Table,
			Key:        row.Key,
			RawData:    row.Data,
			LocalFiles: row.LocalFiles,
		}

		err := tnm.network.Send(peerID, responseMsg)
		if err != nil {
			stdlog.Printf("[MultiTenantManager:%s] 发送行数据失败: %v", tnm.tenantID, err)
		}
	}

	doneMsg := &NetworkMessage{
		Type:      MsgTypeFetchRawResponse,
		RequestID: msg.RequestID,
		Table:     msg.Table,
		Key:       fetchRawResponseDoneKey,
	}
	if err := tnm.network.Send(peerID, doneMsg); err != nil {
		stdlog.Printf("[MultiTenantManager:%s] 发送 fetch 结束标记失败: %v", tnm.tenantID, err)
	}
}
