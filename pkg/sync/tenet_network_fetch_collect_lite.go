package sync

import (
	"fmt"
	"time"
)

func collectFetchRawResponsesLite(responseCh <-chan fetchRawResponseLite, overflowCh <-chan struct{}, timeout time.Duration, idleTimeout time.Duration) ([]RawRowData, error) {
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	if idleTimeout <= 0 {
		idleTimeout = defaultFetchResponseIdleTimeout
	}

	overallTimer := time.NewTimer(timeout)
	defer overallTimer.Stop()

	var idleTimer *time.Timer
	var idleC <-chan time.Time
	stopIdle := func() {
		if idleTimer == nil {
			return
		}
		if !idleTimer.Stop() {
			select {
			case <-idleTimer.C:
			default:
			}
		}
	}
	defer stopIdle()

	resetIdle := func() {
		if idleTimer == nil {
			idleTimer = time.NewTimer(idleTimeout)
			idleC = idleTimer.C
			return
		}
		if !idleTimer.Stop() {
			select {
			case <-idleTimer.C:
			default:
			}
		}
		idleTimer.Reset(idleTimeout)
	}

	rows := make([]RawRowData, 0, cap(responseCh))
	for {
		select {
		case msg, ok := <-responseCh:
			if !ok {
				if len(rows) > 0 {
					return rows, nil
				}
				return nil, fmt.Errorf("%w", ErrResponseChannelClosed)
			}
			if msg.Type == internalMsgTypePeerDisconnected {
				return nil, fmt.Errorf("%w", ErrPeerDisconnectedBeforeResponse)
			}
			if msg.Type != MsgTypeFetchRawResponse {
				continue
			}
			if msg.Key == fetchRawResponseDoneKey {
				return rows, nil
			}
			if msg.Key == "" || msg.RawData == nil {
				continue
			}

			rows = append(rows, RawRowData{
				Key:        msg.Key,
				Data:       msg.RawData,
				LocalFiles: msg.LocalFiles,
			})
			resetIdle()

		case <-idleC:
			if len(rows) > 0 {
				return rows, fmt.Errorf("%w: received %d rows", ErrTimeoutWaitingResponseCompletion, len(rows))
			}
			return nil, fmt.Errorf("%w", ErrTimeoutWaitingResponse)

		case <-overflowCh:
			return nil, fmt.Errorf("%w", ErrResponseOverflow)

		case <-overallTimer.C:
			if len(rows) > 0 {
				return rows, fmt.Errorf("%w: received %d rows", ErrTimeoutWaitingResponseCompletion, len(rows))
			}
			return nil, fmt.Errorf("%w", ErrTimeoutWaitingResponse)
		}
	}
}
