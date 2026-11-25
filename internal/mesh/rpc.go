package mesh

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"sort"
	"time"

	"github.com/killbane1232/muninn/internal/model"
	"github.com/killbane1232/muninn/internal/store"
)

type rpcRequest struct {
	Method string          `json:"method"`
	Args   json.RawMessage `json:"args"`
}
type rpcResponse struct {
	Value json.RawMessage `json:"value,omitempty"`
	Error string          `json:"error,omitempty"`
}
type query struct {
	ID        string `json:"id,omitempty"`
	Login     string `json:"login,omitempty"`
	Signature string `json:"signature,omitempty"`
	Since     int64  `json:"since,omitempty"`
	N         int    `json:"n,omitempty"`
}
type signalRequest struct {
	ID     string       `json:"id"`
	Signal model.Signal `json:"signal"`
}
type reportRequest struct {
	ID     string                   `json:"id"`
	Report model.ChunkReportRequest `json:"report"`
}

func pack(v any, err error) rpcResponse {
	if err != nil {
		code := "unavailable"
		switch {
		case errors.Is(err, store.ErrNotFound), errors.Is(err, store.ErrChunkNotFound):
			code = "not_found"
		case errors.Is(err, store.ErrInvalidSignature):
			code = "signature"
		case errors.Is(err, store.ErrInvalidKey), errors.Is(err, store.ErrInvalidPeer), errors.Is(err, store.ErrInvalidChunk), errors.Is(err, store.ErrNoSigningKey):
			code = "invalid"
		}
		return rpcResponse{Error: code}
	}
	data, err := json.Marshal(v)
	if err != nil {
		return rpcResponse{Error: "unavailable"}
	}
	return rpcResponse{Value: data}
}
func unpack[T any](r rpcResponse) (T, error) {
	var v T
	switch r.Error {
	case "":
	case "not_found":
		return v, store.ErrNotFound
	case "signature":
		return v, store.ErrInvalidSignature
	case "invalid":
		return v, store.ErrInvalidChunk
	default:
		return v, store.ErrUnavailable
	}
	if err := json.Unmarshal(r.Value, &v); err != nil {
		return v, store.ErrUnavailable
	}
	return v, nil
}

func (m *Mesh) rpc(ctx context.Context, body []byte) any {
	var req rpcRequest
	if json.Unmarshal(body, &req) != nil {
		return rpcResponse{Error: "invalid"}
	}
	var q query
	if json.Unmarshal(req.Args, &q) != nil {
		return rpcResponse{Error: "invalid"}
	}
	ctx = m.localContext(ctx)
	// Do not call m.Get/List/etc here: doing so would recursively flood the mesh.
	switch req.Method {
	case "get":
		return pack(m.localPeer(ctx, q.ID))
	case "key":
		return pack(m.Store.GetByKey(ctx, q.Login, q.Signature))
	case "list":
		return pack(m.Store.List(ctx))
	case "best":
		if q.N < 1 || q.N > 1000 {
			return rpcResponse{Error: "invalid"}
		}
		return pack(m.Store.GetBestPeers(ctx, q.N))
	case "thick":
		if q.N < 1 || q.N > 1000 {
			return rpcResponse{Error: "invalid"}
		}
		return pack(m.Store.GetBestThickPeers(ctx, q.N))
	case "file":
		return pack(m.Store.GetChunksByFileID(ctx, q.ID))
	case "recipient":
		return pack(m.Store.GetChunksByRecipient(ctx, q.ID, q.Since))
	case "poll_signals":
		return pack(m.Store.PollSignals(ctx, q.ID))
	case "signal":
		var s signalRequest
		if json.Unmarshal(req.Args, &s) != nil {
			return rpcResponse{Error: "invalid"}
		}
		if _, err := m.Store.Get(ctx, s.ID); err != nil {
			return pack(nil, err)
		}
		return pack(struct{}{}, m.signal(ctx, s.ID, s.Signal))
	case "confirm":
		var r model.ConfirmChunkRequest
		if json.Unmarshal(req.Args, &r) != nil {
			return rpcResponse{Error: "invalid"}
		}
		return pack(m.Store.ConfirmChunk(ctx, r))
	case "read":
		var r model.ReadChunkRequest
		if json.Unmarshal(req.Args, &r) != nil {
			return rpcResponse{Error: "invalid"}
		}
		return pack(m.Store.ReadChunk(ctx, r))
	case "report":
		var r reportRequest
		if json.Unmarshal(req.Args, &r) != nil {
			return rpcResponse{Error: "invalid"}
		}
		return pack(m.Store.ReportChunk(ctx, r.ID, r.Report))
	default:
		return rpcResponse{Error: "invalid"}
	}
}

func call[T any](m *Mesh, ctx context.Context, node, method string, args any) (T, error) {
	var v T
	body, err := json.Marshal(args)
	if err != nil {
		return v, err
	}
	var resp rpcResponse
	if err := m.exchange(ctx, node, "/api/v1/mesh/rpc", rpcRequest{Method: method, Args: body}, &resp); err != nil {
		return v, fmt.Errorf("%w: %s", store.ErrUnavailable, node)
	}
	return unpack[T](resp)
}

// Results retain node order so a collision cannot randomly replace local identity.
func collect[T any](m *Mesh, ctx context.Context, method string, args any, local func(context.Context) (T, error)) ([]T, error) {
	ctx, cancel := context.WithTimeout(ctx, m.cfg.transitTimeout())
	defer cancel()
	nodes := m.Nodes()
	type result struct {
		index int
		value T
		err   error
	}
	ch := make(chan result, len(nodes)+1)
	go func() { v, err := local(m.localContext(ctx)); ch <- result{0, v, err} }()
	for i, node := range nodes {
		go func(i int, node string) { v, err := call[T](m, ctx, node, method, args); ch <- result{i + 1, v, err} }(i, node)
	}
	ordered := make([]result, len(nodes)+1)
	for i := range ordered {
		ordered[i].err = store.ErrUnavailable
	}
wait:
	for range ordered {
		select {
		case r := <-ch:
			ordered[r.index] = r
		case <-ctx.Done():
			break wait
		}
	}
	out := []T{}
	var failure error
	for _, r := range ordered {
		if r.err == nil {
			out = append(out, r.value)
		} else if !errors.Is(r.err, store.ErrNotFound) && !errors.Is(r.err, store.ErrChunkNotFound) {
			failure = errors.Join(failure, r.err)
		}
	}
	return out, failure
}

func (m *Mesh) Get(ctx context.Context, id string) (model.Peer, error) {
	p, err := m.Store.Get(ctx, id)
	if err == nil || !errors.Is(err, store.ErrNotFound) {
		return p, err
	}
	values, err := collect(m, ctx, "get", query{ID: id}, func(ctx context.Context) (model.Peer, error) { return m.Store.Get(ctx, id) })
	for _, p := range values {
		if p.ID == id {
			return p, nil
		}
	}
	if err != nil {
		return model.Peer{}, err
	}
	return model.Peer{}, store.ErrNotFound
}
func mergePeers(groups [][]model.Peer) []model.Peer {
	out := []model.Peer{}
	seen := map[string]bool{}
	for _, group := range groups {
		for _, p := range group {
			if p.ID != "" && !seen[p.ID] {
				seen[p.ID] = true
				out = append(out, p)
			}
		}
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}
func (m *Mesh) GetByKey(ctx context.Context, login, signature string) ([]model.Peer, error) {
	groups, err := collect(m, ctx, "key", query{Login: login, Signature: signature}, func(ctx context.Context) ([]model.Peer, error) { return m.Store.GetByKey(ctx, login, signature) })
	out := []model.Peer{}
	for _, p := range mergePeers(groups) {
		if p.Login == login && p.SignatureKey == signature {
			out = append(out, p)
		}
	}
	if len(out) > 0 {
		return out, nil
	}
	if err != nil {
		return nil, err
	}
	return nil, store.ErrNotFound
}
func (m *Mesh) List(ctx context.Context) ([]model.Peer, error) {
	groups, err := collect(m, ctx, "list", query{}, m.Store.List)
	if len(groups) == 0 && err != nil {
		return nil, err
	}
	return mergePeers(groups), nil
}
func (m *Mesh) best(ctx context.Context, n int, thick bool) ([]model.Peer, error) {
	if n <= 0 {
		return []model.Peer{}, nil
	}
	n = min(n, 1000)
	method := "best"
	local := m.Store.GetBestPeers
	if thick {
		method = "thick"
		local = m.Store.GetBestThickPeers
	}
	groups, err := collect(m, ctx, method, query{N: n}, func(ctx context.Context) ([]model.Peer, error) { return local(ctx, n) })
	if len(groups) == 0 && err != nil {
		return nil, err
	}
	out := mergePeers(groups)
	sort.SliceStable(out, func(i, j int) bool { return store.EffectiveScore(out[i]) > store.EffectiveScore(out[j]) })
	return out[:min(n, len(out))], nil
}
func (m *Mesh) GetBestPeers(ctx context.Context, n int) ([]model.Peer, error) {
	return m.best(ctx, n, false)
}
func (m *Mesh) GetBestThickPeers(ctx context.Context, n int) ([]model.Peer, error) {
	return m.best(ctx, n, true)
}

func mergeChunks(groups [][]model.ChunkRecord) []model.ChunkRecord {
	out := []model.ChunkRecord{}
	seen := map[string]int{}
	for _, group := range groups {
		for _, r := range group {
			if !r.Persist && r.TTL > 0 && r.CreatedAt+int64(r.TTL) <= time.Now().Unix() {
				continue
			}
			key := url.QueryEscape(r.FileID) + ":" + fmt.Sprint(r.ChunkIndex) + ":" + url.QueryEscape(r.PeerID) + ":" + url.QueryEscape(r.SenderID) + ":" + url.QueryEscape(r.RecipientID) + ":" + r.Hash
			if i, ok := seen[key]; ok {
				if r.UpdatedAt > out[i].UpdatedAt {
					out[i] = r
				}
				continue
			}
			seen[key] = len(out)
			out = append(out, r)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		a, b := out[i], out[j]
		if a.FileID != b.FileID {
			return a.FileID < b.FileID
		}
		if a.ChunkIndex != b.ChunkIndex {
			return a.ChunkIndex < b.ChunkIndex
		}
		return a.PeerID < b.PeerID
	})
	return out
}
func (m *Mesh) GetChunksByFileID(ctx context.Context, id string) ([]model.ChunkRecord, error) {
	groups, err := collect(m, ctx, "file", query{ID: id}, func(ctx context.Context) ([]model.ChunkRecord, error) { return m.Store.GetChunksByFileID(ctx, id) })
	if len(groups) == 0 && err != nil {
		return nil, err
	}
	return mergeChunks(groups), nil
}
func (m *Mesh) GetChunksByRecipient(ctx context.Context, id string, since int64) ([]model.ChunkRecord, error) {
	// A client's scalar cursor cannot represent newly discovered nodes or clocks
	// on different servers. All reads replay the live snapshot, allowing a
	// recovered node's old records to be delivered even after the cursor moved.
	groups, err := collect(m, ctx, "recipient", query{ID: id}, func(ctx context.Context) ([]model.ChunkRecord, error) {
		return m.Store.GetChunksByRecipient(ctx, id, 0)
	})
	if len(groups) == 0 && err != nil {
		return nil, err
	}
	return mergeChunks(groups), nil
}
func (m *Mesh) localPeer(ctx context.Context, id string) (model.Peer, error) {
	peer, err := m.Store.Get(ctx, id)
	if err == nil && m.SignalConnected != nil {
		peer.SignalConnected = m.SignalConnected(id)
	}
	return peer, err
}

func (m *Mesh) PollSignals(ctx context.Context, id string) ([]model.Signal, error) {
	groups, err := collect(m, ctx, "poll_signals", query{ID: id}, func(ctx context.Context) ([]model.Signal, error) { return m.Store.PollSignals(ctx, id) })
	if len(groups) == 0 && err != nil {
		return nil, err
	}
	signals := []model.Signal{}
	for _, group := range groups {
		signals = append(signals, group...)
	}
	return signals, nil
}

func (m *Mesh) SetSignal(ctx context.Context, id string, sig model.Signal) error {
	local, localErr := m.localPeer(ctx, id)
	if localErr == nil && local.SignalConnected {
		return m.signal(ctx, id, sig)
	}
	if localErr != nil && !errors.Is(localErr, store.ErrNotFound) {
		return localErr
	}
	ctx, cancel := context.WithTimeout(ctx, m.cfg.transitTimeout())
	defer cancel()
	nodes := m.Nodes()
	type owner struct {
		node string
		peer model.Peer
		err  error
	}
	ch := make(chan owner, len(nodes))
	for _, node := range nodes {
		go func(node string) {
			peer, err := call[model.Peer](m, ctx, node, "get", query{ID: id})
			ch <- owner{node, peer, err}
		}(node)
	}
	var best *owner
	if localErr == nil {
		best = &owner{peer: local}
	}
	var failure error
	for range nodes {
		select {
		case candidate := <-ch:
			if candidate.err == nil && candidate.peer.ID == id {
				// The live socket wins over a stale copy of the same endpoint on the old
				// home. Only one selected server has the client's WebSocket at a time.
				if candidate.peer.SignalConnected {
					_, err := call[struct{}](m, ctx, candidate.node, "signal", signalRequest{ID: id, Signal: sig})
					return err
				}
				if best == nil || candidate.peer.LastSeen.After(best.peer.LastSeen) {
					best = &candidate
				}
			} else if candidate.err != nil && !errors.Is(candidate.err, store.ErrNotFound) {
				failure = candidate.err
			}
		case <-ctx.Done():
			return store.ErrUnavailable
		}
	}
	if best != nil {
		if best.node == "" {
			return m.signal(ctx, id, sig)
		}
		_, err := call[struct{}](m, ctx, best.node, "signal", signalRequest{ID: id, Signal: sig})
		return err
	}
	if failure != nil {
		return failure
	}
	return store.ErrNotFound
}
func (m *Mesh) ConfirmChunk(ctx context.Context, req model.ConfirmChunkRequest) (model.ConfirmChunkResult, error) {
	values, err := collect(m, ctx, "confirm", req, func(ctx context.Context) (model.ConfirmChunkResult, error) { return m.Store.ConfirmChunk(ctx, req) })
	if err != nil {
		return model.ConfirmChunkResult{}, err
	}
	if len(values) == 0 {
		return model.ConfirmChunkResult{}, store.ErrChunkNotFound
	}
	return values[0], nil
}
func (m *Mesh) ReadChunk(ctx context.Context, req model.ReadChunkRequest) (model.ReadChunkResult, error) {
	values, err := collect(m, ctx, "read", req, func(ctx context.Context) (model.ReadChunkResult, error) { return m.Store.ReadChunk(ctx, req) })
	if err != nil {
		return model.ReadChunkResult{}, err
	}
	if len(values) == 0 {
		return model.ReadChunkResult{}, store.ErrChunkNotFound
	}
	return values[0], nil
}
func (m *Mesh) ReportChunk(ctx context.Context, id string, req model.ChunkReportRequest) (model.ChunkReportResult, error) {
	values, err := collect(m, ctx, "report", reportRequest{ID: id, Report: req}, func(ctx context.Context) (model.ChunkReportResult, error) { return m.Store.ReportChunk(ctx, id, req) })
	if err != nil {
		return model.ChunkReportResult{}, err
	}
	if len(values) == 0 {
		return model.ChunkReportResult{}, store.ErrChunkNotFound
	}
	return values[0], nil
}
