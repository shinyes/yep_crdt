package sync

import "errors"

var (
	// ErrPeerDisconnectedBeforeResponse indicates request/response flow broke on peer disconnect.
	ErrPeerDisconnectedBeforeResponse = errors.New("peer disconnected before response")
	// ErrTimeoutWaitingResponse indicates no response arrived before timeout.
	ErrTimeoutWaitingResponse = errors.New("timeout waiting for response")
	// ErrTimeoutWaitingResponseCompletion indicates partial rows arrived but stream never completed.
	ErrTimeoutWaitingResponseCompletion = errors.New("timeout waiting for response completion")
	// ErrResponseOverflow indicates receiver-side response buffering overflowed.
	ErrResponseOverflow = errors.New("response overflow while collecting fetch_raw_response")
	// ErrResponseChannelClosed indicates waiter channel closed before any data.
	ErrResponseChannelClosed = errors.New("response channel closed before data arrived")
)
