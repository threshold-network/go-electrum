package electrum

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
)

// SubscribeHeadersResp represent the response to SubscribeHeaders().
type SubscribeHeadersResp struct {
	Result *SubscribeHeadersResult `json:"result"`
}

// SubscribeHeadersNotif represent the notification to SubscribeHeaders().
type SubscribeHeadersNotif struct {
	Params []*SubscribeHeadersResult `json:"params"`
}

// SubscribeHeadersResult represents the content of the result field in the response to SubscribeHeaders().
type SubscribeHeadersResult struct {
	Height int32  `json:"height,omitempty"`
	Hex    string `json:"hex"`
}

// SubscribeHeaders subscribes to receive block headers notifications when new blocks are found.
//
// Cancel ctx when no longer reading notifications. The returned channel closes
// when ctx is canceled, the client shuts down, or a notification is invalid.
// Use SubscribeHeadersSingle when only the current tip is needed.
//
// https://electrumx.readthedocs.io/en/latest/protocol-methods.html#blockchain-headers-subscribe
func (s *Client) SubscribeHeaders(ctx context.Context) (<-chan *SubscribeHeadersResult, error) {
	notifications, unsubscribe := s.listenPush("blockchain.headers.subscribe")
	var resp SubscribeHeadersResp

	err := s.request(ctx, "blockchain.headers.subscribe", []interface{}{}, &resp)
	if err != nil {
		unsubscribe()
		return nil, err
	}

	respChan := make(chan *SubscribeHeadersResult, 1)
	respChan <- resp.Result

	go func() {
		defer close(respChan)
		defer unsubscribe()
		for {
			select {
			case <-ctx.Done():
				return
			case <-s.quit:
				return
			case msg, ok := <-notifications:
				if !ok || msg.err != nil {
					return
				}
				var resp SubscribeHeadersNotif
				if err := json.Unmarshal(msg.content, &resp); err != nil {
					return
				}
				for _, param := range resp.Params {
					select {
					case respChan <- param:
					case <-ctx.Done():
						return
					case <-s.quit:
						return
					}
				}
			}
		}
	}()

	return respChan, nil
}

// SubscribeHeadersSingle subscribes to receive the header of the current
// blockchain tip. Unlike SubscribeHeaders, this method only returns the tip
// and does not listen for new block headers.
//
// Worth noting that this action still creates a new subscription in the Electrum
// server. The protocol does neither support a single-shot request for the
// current blockchain tip nor subscription cancellation. Although this limitation
// causes a slight resource overhead on the client, this method does not spawn
// a goroutine or require the caller to manage a streaming subscription.
//
// https://electrumx.readthedocs.io/en/latest/protocol-methods.html#blockchain-headers-subscribe
func (s *Client) SubscribeHeadersSingle(ctx context.Context) (
	*SubscribeHeadersResult,
	error,
) {
	var resp SubscribeHeadersResp

	err := s.request(
		ctx,
		"blockchain.headers.subscribe",
		[]interface{}{},
		&resp,
	)
	if err != nil {
		return nil, err
	}

	return resp.Result, nil
}

// ScripthashSubscription ...
type ScripthashSubscription struct {
	server    *Client
	notifChan chan *SubscribeNotif

	subscribedSH  []string
	scripthashMap map[string]string

	lock       sync.RWMutex
	ctx        context.Context
	cancel     context.CancelFunc
	notifyLock sync.RWMutex
	closed     bool
}

// SubscribeNotif represent the notification to SubscribeScripthash() and SubscribeMasternode().
type SubscribeNotif struct {
	Params [2]string `json:"params"`
}

// SubscribeScripthash receives scripthash notifications until Close or client
// shutdown. Use SubscribeScripthashContext to tie the subscription to a context.
func (s *Client) SubscribeScripthash() (*ScripthashSubscription, <-chan *SubscribeNotif) {
	return s.SubscribeScripthashContext(context.Background())
}

// SubscribeScripthashContext receives scripthash notifications until ctx is
// canceled, Close is called, or the client shuts down. Cancel or close the
// subscription when no longer reading its channel.
func (s *Client) SubscribeScripthashContext(ctx context.Context) (*ScripthashSubscription, <-chan *SubscribeNotif) {
	ctx, cancel := context.WithCancel(ctx)
	sub := &ScripthashSubscription{
		server:        s,
		notifChan:     make(chan *SubscribeNotif, 1),
		scripthashMap: make(map[string]string),
		ctx:           ctx,
		cancel:        cancel,
	}
	notifications, unsubscribe := s.listenPush("blockchain.scripthash.subscribe")

	go func() {
		defer func() {
			cancel()
			unsubscribe()
			// Add can also send initial notifications. Wake blocked senders before
			// taking the exclusive lock and closing their destination channel.
			sub.notifyLock.Lock()
			sub.closed = true
			close(sub.notifChan)
			sub.notifyLock.Unlock()
		}()
		for {
			select {
			case <-ctx.Done():
				return
			case <-s.quit:
				return
			case msg, ok := <-notifications:
				if !ok || msg.err != nil {
					return
				}
				var resp SubscribeNotif
				if err := json.Unmarshal(msg.content, &resp); err != nil {
					return
				}
				sub.lock.RLock()
				subscribed := false
				for _, a := range sub.subscribedSH {
					if a == resp.Params[0] {
						subscribed = true
						break
					}
				}
				sub.lock.RUnlock()
				if subscribed {
					if err := sub.notify(ctx, &resp); err != nil {
						return
					}
				}
			}
		}
	}()

	return sub, sub.notifChan
}

// Close stops this subscription and closes its notification channel once
// in-flight sends have exited. It is safe to call more than once.
func (sub *ScripthashSubscription) Close() {
	sub.cancel()
}

func (sub *ScripthashSubscription) notify(ctx context.Context, notification *SubscribeNotif) error {
	sub.notifyLock.RLock()
	defer sub.notifyLock.RUnlock()
	if sub.closed || sub.ctx.Err() != nil {
		return context.Canceled
	}
	select {
	case <-sub.ctx.Done():
		return sub.ctx.Err()
	case <-sub.server.quit:
		return ErrServerShutdown
	case <-ctx.Done():
		return ErrTimeout
	case sub.notifChan <- notification:
		return nil
	}
}

// Add ...
func (sub *ScripthashSubscription) Add(ctx context.Context, scripthash string, address ...string) error {
	if err := sub.ctx.Err(); err != nil {
		return err
	}
	// End the RPC when either the caller or the subscription cancels. Join the
	// cancellation watcher on every return path so it cannot outlive this Add.
	requestCtx, cancelRequest := context.WithCancel(ctx)
	watcherDone := make(chan struct{})
	go func() {
		defer close(watcherDone)
		select {
		case <-sub.ctx.Done():
			cancelRequest()
		case <-requestCtx.Done():
		}
	}()
	defer func() {
		cancelRequest()
		<-watcherDone
	}()
	var resp basicResp

	err := sub.server.request(requestCtx, "blockchain.scripthash.subscribe", []interface{}{scripthash}, &resp)
	if err != nil {
		if err == ErrTimeout && sub.ctx.Err() != nil {
			return sub.ctx.Err()
		}
		return err
	}

	// Publish the initial status before the push worker can observe this script
	// as active. Never hold the subscription state lock while waiting on a reader.
	if len(resp.Result) > 0 {
		if err := sub.notify(ctx, &SubscribeNotif{[2]string{scripthash, resp.Result}}); err != nil {
			return err
		}
	}

	sub.lock.Lock()
	defer sub.lock.Unlock()
	if err := sub.ctx.Err(); err != nil {
		return err
	}
	sub.subscribedSH = append(sub.subscribedSH[:], scripthash)
	if len(address) > 0 {
		sub.scripthashMap[scripthash] = address[0]
	}

	return nil
}

// GetAddress ...
func (sub *ScripthashSubscription) GetAddress(scripthash string) (string, error) {
	sub.lock.RLock()
	defer sub.lock.RUnlock()
	address, ok := sub.scripthashMap[scripthash]
	if ok {
		return address, nil
	}

	return "", errors.New("scripthash not found in map")
}

// GetScripthash ...
func (sub *ScripthashSubscription) GetScripthash(address string) (string, error) {
	sub.lock.RLock()
	defer sub.lock.RUnlock()
	var found bool
	var scripthash string

	for k, v := range sub.scripthashMap {
		if v == address {
			scripthash = k
			found = true
		}
	}

	if found {
		return scripthash, nil
	}

	return "", errors.New("address not found in map")
}

// GetChannel ...
func (sub *ScripthashSubscription) GetChannel() <-chan *SubscribeNotif {
	return sub.notifChan
}

// Remove ...
func (sub *ScripthashSubscription) Remove(scripthash string) error {
	sub.lock.Lock()
	defer sub.lock.Unlock()
	for i, v := range sub.subscribedSH {
		if v == scripthash {
			sub.subscribedSH = append(sub.subscribedSH[:i], sub.subscribedSH[i+1:]...)
			return nil
		}
	}

	return errors.New("scripthash not found")
}

// RemoveAddress ...
func (sub *ScripthashSubscription) RemoveAddress(address string) error {
	scripthash, err := sub.GetScripthash(address)
	if err != nil {
		return err
	}

	sub.lock.Lock()
	defer sub.lock.Unlock()
	for i, v := range sub.subscribedSH {
		if v == scripthash {
			sub.subscribedSH = append(sub.subscribedSH[:i], sub.subscribedSH[i+1:]...)
			delete(sub.scripthashMap, scripthash)
			return nil
		}
	}

	return errors.New("scripthash not found")
}

// Resubscribe ...
func (sub *ScripthashSubscription) Resubscribe(ctx context.Context) error {
	sub.lock.RLock()
	scripthashes := append([]string(nil), sub.subscribedSH...)
	sub.lock.RUnlock()
	for _, v := range scripthashes {
		err := sub.Add(ctx, v)
		if err != nil {
			return err
		}
	}

	return nil
}

// SubscribeMasternode subscribes to receive notifications when a masternode status changes.
// Cancel ctx when no longer reading. Cancellation or client shutdown closes the channel.
// https://electrumx.readthedocs.io/en/latest/protocol-methods.html#blockchain-headers-subscribe
func (s *Client) SubscribeMasternode(ctx context.Context, collateral string) (<-chan string, error) {
	notifications, unsubscribe := s.listenPush("blockchain.masternode.subscribe")
	var resp basicResp

	err := s.request(ctx, "blockchain.masternode.subscribe", []interface{}{collateral}, &resp)
	if err != nil {
		unsubscribe()
		return nil, err
	}

	respChan := make(chan string, 1)
	if len(resp.Result) > 0 {
		respChan <- resp.Result
	}

	go func() {
		defer close(respChan)
		defer unsubscribe()
		for {
			select {
			case <-ctx.Done():
				return
			case <-s.quit:
				return
			case msg, ok := <-notifications:
				if !ok || msg.err != nil {
					return
				}
				var resp SubscribeNotif
				if err := json.Unmarshal(msg.content, &resp); err != nil {
					return
				}
				for _, param := range resp.Params {
					select {
					case respChan <- param:
					case <-ctx.Done():
						return
					case <-s.quit:
						return
					}
				}
			}
		}
	}()

	return respChan, nil
}
