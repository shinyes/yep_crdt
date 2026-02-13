package sync

import (
	"context"
	"encoding/json"
	"fmt"
	stdlog "log"
	"sync"
	"time"

	"github.com/shinyes/tenet/api"
	tenetlog "github.com/shinyes/tenet/log"
)

// TenantNetwork 租户网络（对应一个租户/数据库）
// 每个租户有独立的 tenet 频道，实现租户间的数据隔离
type TenantNetwork struct {
	tenantID string       // 租户 ID (DatabaseID)
	config   *TenetConfig // 配置
	tunnel   *api.Tunnel  // tenet 隧道
	ctx      context.Context
	cancel   context.CancelFunc

	mu               sync.RWMutex
	peerHandlers     map[string]PeerMessageHandler   // peerID -> 消息处理器
	broadcastHandler PeerMessageHandler              // 广播消息处理器（用于接收所有消息）
	responseChannels map[string]chan *NetworkMessage // 请求ID -> 响应通道
}

// TenetConfig tenet 网络配置
type TenetConfig struct {
	Password    string   // 网络密码（必须）
	ListenPort  int      // 监听端口，0 表示随机
	RelayNodes  []string // 中继节点地址
	EnableDebug bool     // 是否启用调试日志
}

// PeerMessageHandler peer 消息处理器
type PeerMessageHandler struct {
	OnReceive func(peerID string, msg *NetworkMessage)
}

// NewTenantNetwork 创建租户网络
// tenantID: 租户 ID，将作为 tenet 频道 ID
func NewTenantNetwork(tenantID string, config *TenetConfig) (*TenantNetwork, error) {
	if config == nil || config.Password == "" {
		return nil, fmt.Errorf("password is required")
	}

	// 租户 ID 作为频道 ID，实现租户隔离
	channelID := tenantID

	opts := []api.Option{
		api.WithPassword(config.Password),
		api.WithListenPort(config.ListenPort),
		api.WithChannelID(channelID),
	}

	// 添加中继节点
	if len(config.RelayNodes) > 0 {
		opts = append(opts, api.WithRelayNodes(config.RelayNodes))
	}

	// 启用日志
	if config.EnableDebug {
		logger := tenetlog.NewStdLogger(
			tenetlog.WithLevel(tenetlog.LevelDebug),
			tenetlog.WithPrefix(fmt.Sprintf("[tenet:%s]", tenantID)),
		)
		opts = append(opts, api.WithLogger(logger))
	} else {
		opts = append(opts, api.WithLogger(tenetlog.Nop()))
	}

	// 创建隧道
	tunnel, err := api.NewTunnel(opts...)
	if err != nil {
		return nil, fmt.Errorf("create tunnel failed: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	tn := &TenantNetwork{
		tenantID:         tenantID,
		config:           config,
		tunnel:           tunnel,
		ctx:              ctx,
		cancel:           cancel,
		peerHandlers:     make(map[string]PeerMessageHandler),
		broadcastHandler: PeerMessageHandler{},
		responseChannels: make(map[string]chan *NetworkMessage),
	}

	// 设置回调
	tn.setupCallbacks()

	return tn, nil
}

// setupCallbacks 设置回调
func (tn *TenantNetwork) setupCallbacks() {
	// 接收数据回调
	tn.tunnel.OnReceive(func(peerID string, data []byte) {
		tn.handleReceive(peerID, data)
	})

	// 节点连接回调
	tn.tunnel.OnPeerConnected(func(peerID string) {
		stdlog.Printf("[TenantNetwork:%s] 节点连接: %s", tn.tenantID, peerID)
	})

	// 节点断开回调
	tn.tunnel.OnPeerDisconnected(func(peerID string) {
		stdlog.Printf("[TenantNetwork:%s] 节点断开: %s", tn.tenantID, peerID)
		// 清理该 peer 的响应通道
		tn.mu.Lock()
		delete(tn.responseChannels, peerID)
		tn.mu.Unlock()
	})
}

// handleReceive 处理接收到的数据
func (tn *TenantNetwork) handleReceive(peerID string, data []byte) {
	// 解析消息
	var msg NetworkMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		stdlog.Printf("[TenantNetwork:%s] 解析消息失败: %v", tn.tenantID, err)
		return
	}

	// 如果有请求ID，说明是响应消息
	if msg.RequestID != "" {
		tn.mu.RLock()
		ch, ok := tn.responseChannels[msg.RequestID]
		tn.mu.RUnlock()

		if ok {
			select {
			case ch <- &msg:
			default:
				stdlog.Printf("[TenantNetwork:%s] 响应通道已满或已关闭", tn.tenantID)
			}
		}
		return
	}

	// 调用 peer 处理器
	tn.mu.RLock()
	handler, ok := tn.peerHandlers[peerID]
	broadcastHandler := tn.broadcastHandler
	tn.mu.RUnlock()

	// 先尝试特定 peer 处理器
	if ok && handler.OnReceive != nil {
		handler.OnReceive(peerID, &msg)
	} else if broadcastHandler.OnReceive != nil {
		// 然后尝试广播处理器（用于接收所有消息）
		broadcastHandler.OnReceive(peerID, &msg)
	}
}

// Start 启动租户网络
func (tn *TenantNetwork) Start() error {
	stdlog.Printf("[TenantNetwork:%s] 启动隧道，监听端口: %d", tn.tenantID, tn.config.ListenPort)
	if err := tn.tunnel.Start(); err != nil {
		return fmt.Errorf("start tunnel failed: %w", err)
	}
	stdlog.Printf("[TenantNetwork:%s] 本地节点 ID: %s", tn.tenantID, tn.tunnel.LocalID())
	stdlog.Printf("[TenantNetwork:%s] 本地监听地址: %s", tn.tenantID, tn.tunnel.LocalAddr())
	return nil
}

// Stop 停止租户网络
func (tn *TenantNetwork) Stop() {
	stdlog.Printf("[TenantNetwork:%s] 停止隧道", tn.tenantID)
	tn.cancel()
	tn.tunnel.GracefulStop()
}

// Connect 连接到其他节点
func (tn *TenantNetwork) Connect(addr string) error {
	stdlog.Printf("[TenantNetwork:%s] 连接到节点: %s", tn.tenantID, addr)
	return tn.tunnel.Connect(addr)
}

// LocalID 获取本地节点 ID
func (tn *TenantNetwork) LocalID() string {
	return tn.tunnel.LocalID()
}

// LocalAddr 获取本地监听地址
func (tn *TenantNetwork) LocalAddr() string {
	return tn.tunnel.LocalAddr()
}

// Peers 获取已连接节点列表
func (tn *TenantNetwork) Peers() []string {
	return tn.tunnel.Peers()
}

// TenantID 获取租户 ID
func (tn *TenantNetwork) TenantID() string {
	return tn.tenantID
}

// SetPeerHandler 设置 peer 消息处理器
func (tn *TenantNetwork) SetPeerHandler(peerID string, handler PeerMessageHandler) {
	tn.mu.Lock()
	tn.peerHandlers[peerID] = handler
	tn.mu.Unlock()
}

// SetBroadcastHandler 设置广播消息处理器（用于接收所有消息）
func (tn *TenantNetwork) SetBroadcastHandler(handler PeerMessageHandler) {
	tn.mu.Lock()
	tn.broadcastHandler = handler
	tn.mu.Unlock()
}

// RemovePeerHandler 移除 peer 消息处理器
func (tn *TenantNetwork) RemovePeerHandler(peerID string) {
	tn.mu.Lock()
	delete(tn.peerHandlers, peerID)
	delete(tn.responseChannels, peerID)
	tn.mu.Unlock()
}

// Send 发送消息到指定节点
func (tn *TenantNetwork) Send(peerID string, msg *NetworkMessage) error {
	msg.TenantID = tn.tenantID
	msg.NodeID = tn.tunnel.LocalID()
	// 只有当时间戳为0时才设置，使用调用者传入的时间戳
	if msg.Timestamp == 0 {
		msg.Timestamp = time.Now().UnixMilli()
	}

	payload, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	return tn.tunnel.Send(tn.tenantID, peerID, payload)
}

// Broadcast 广播消息到频道内所有节点
func (tn *TenantNetwork) Broadcast(msg *NetworkMessage) (int, error) {
	msg.TenantID = tn.tenantID
	msg.NodeID = tn.tunnel.LocalID()
	// 只有当时间戳为0时才设置，使用调用者传入的时间戳
	if msg.Timestamp == 0 {
		msg.Timestamp = time.Now().UnixMilli()
	}

	payload, err := json.Marshal(msg)
	if err != nil {
		return 0, err
	}

	return tn.tunnel.Broadcast(tn.tenantID, payload)
}

// SendWithResponse 发送消息并等待响应
func (tn *TenantNetwork) SendWithResponse(peerID string, msg *NetworkMessage, timeout time.Duration) (*NetworkMessage, error) {
	// 生成请求 ID
	requestID := fmt.Sprintf("%s-%d", tn.tunnel.LocalID(), time.Now().UnixNano())
	msg.RequestID = requestID

	// 创建响应通道
	responseCh := make(chan *NetworkMessage, 1)
	tn.mu.Lock()
	tn.responseChannels[requestID] = responseCh
	tn.mu.Unlock()

	// 确保清理
	defer func() {
		tn.mu.Lock()
		delete(tn.responseChannels, requestID)
		tn.mu.Unlock()
	}()

	// 发送消息
	if err := tn.Send(peerID, msg); err != nil {
		return nil, err
	}

	// 等待响应
	select {
	case response := <-responseCh:
		return response, nil
	case <-time.After(timeout):
		return nil, fmt.Errorf("timeout waiting for response")
	}
}

// SendHeartbeat 发送心跳
func (tn *TenantNetwork) SendHeartbeat(targetNodeID string, clock int64) error {
	msg := &NetworkMessage{
		Type:  MsgTypeHeartbeat,
		Clock: clock,
	}
	return tn.Send(targetNodeID, msg)
}

// BroadcastHeartbeat 广播心跳
func (tn *TenantNetwork) BroadcastHeartbeat(clock int64) error {
	msg := &NetworkMessage{
		Type:      MsgTypeHeartbeat,
		Clock:     clock,
		Timestamp: clock, // 使用 HLC 时钟作为时间戳
	}
	_, err := tn.Broadcast(msg)
	return err
}

// SendRawData 发送原始 CRDT 字节到指定节点
func (tn *TenantNetwork) SendRawData(targetNodeID string, table string, key string, rawData []byte, timestamp int64) error {
	msg := &NetworkMessage{
		Type:      MsgTypeRawData,
		Table:     table,
		Key:       key,
		RawData:   rawData,
		Timestamp: timestamp,
	}
	return tn.Send(targetNodeID, msg)
}

// BroadcastRawData 广播原始 CRDT 字节
func (tn *TenantNetwork) BroadcastRawData(table string, key string, rawData []byte, timestamp int64) error {
	msg := &NetworkMessage{
		Type:      MsgTypeRawData,
		Table:     table,
		Key:       key,
		RawData:   rawData,
		Timestamp: timestamp,
	}
	count, err := tn.Broadcast(msg)
	if err != nil {
		stdlog.Printf("[TenantNetwork:%s] 广播原始数据失败: %v", tn.tenantID, err)
	} else {
		stdlog.Printf("[TenantNetwork:%s] 已广播原始数据到 %d 个节点: table=%s, key=%s", tn.tenantID, count, table, key)
	}
	return err
}

// FetchRawTableData 获取远程节点指定表的所有原始数据
func (tn *TenantNetwork) FetchRawTableData(sourceNodeID string, tableName string) ([]RawRowData, error) {
	return tn.FetchRawTableDataWithTimeout(sourceNodeID, tableName, 30*time.Second)
}

// FetchRawTableDataWithTimeout 获取远程节点原始表数据（带超时）
func (tn *TenantNetwork) FetchRawTableDataWithTimeout(sourceNodeID string, tableName string, timeout time.Duration) ([]RawRowData, error) {
	msg := &NetworkMessage{
		Type:      MsgTypeFetchRawRequest,
		Table:     tableName,
		Timestamp: time.Now().UnixMilli(),
	}

	response, err := tn.SendWithResponse(sourceNodeID, msg, timeout)
	if err != nil {
		return nil, err
	}

	// 解析响应中的原始数据
	if response.RawData == nil {
		return nil, nil
	}

	// 响应中包含单行数据
	// 注意：全量同步会发送多条 FetchRawResponse 消息，
	// 这里只能通过 SendWithResponse 获取第一条。
	// 实际的批量同步由 MultiTenantManager 的消息处理逻辑完成。
	result := []RawRowData{
		{
			Key:  response.Key,
			Data: response.RawData,
		},
	}
	return result, nil
}

// 确保 TenantNetwork 实现 NetworkInterface 接口
var _ NetworkInterface = (*TenantNetwork)(nil)
