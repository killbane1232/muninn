package webrtc

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"

	pion "github.com/pion/webrtc/v3"

	"github.com/killbane1232/muninn/internal/model"
	"github.com/killbane1232/muninn/internal/store"
)

const (
	rpcMethodSignalRelay    = "signal_relay"
	rpcMethodConnectToPeer  = "connect_to_peer"
	rpcNotifyIncomingSignal = "incoming_signal"
)

type rpcRequest struct {
	ID     string          `json:"id"`
	Method string          `json:"method"`
	Params json.RawMessage `json:"params"`
}

type rpcResponse struct {
	ID     string          `json:"id"`
	Result json.RawMessage `json:"result,omitempty"`
	Error  string          `json:"error,omitempty"`
}

type rpcNotification struct {
	Method string          `json:"method"`
	Params json.RawMessage `json:"params"`
}

type SignalRelayRequest struct {
	TargetID string `json:"target_id"`
	From     string `json:"from"`
	Type     string `json:"type"`
	Data     string `json:"data"`
}

type ConnectToPeerRequest struct {
	TargetID string `json:"target_id"`
	Offer    string `json:"offer"`
}

type IncomingSignal struct {
	From string `json:"from"`
	Type string `json:"type"`
	Data string `json:"data"`
}

type peerConn struct {
	pc     *pion.PeerConnection
	dc     *pion.DataChannel
	peerID string
}

type Handler struct {
	store  store.Store
	mu     sync.RWMutex
	peers  map[string]*peerConn
	config pion.Configuration
}

func NewHandler(st store.Store, iceServers []pion.ICEServer) *Handler {
	if iceServers == nil {
		iceServers = defaultICEServers()
	}
	return &Handler{
		store: st,
		peers: make(map[string]*peerConn),
		config: pion.Configuration{
			ICEServers: iceServers,
		},
	}
}

func defaultICEServers() []pion.ICEServer {
	return []pion.ICEServer{
		{URLs: []string{"stun:stun.l.google.com:19302"}},
		{URLs: []string{"stun:stun1.l.google.com:19302"}},
		{URLs: []string{"stun:stun2.l.google.com:19302"}},
		{URLs: []string{"stun:stun3.l.google.com:19302"}},
		{URLs: []string{"stun:stun4.l.google.com:19302"}},
		{URLs: []string{"stun:stun.ekiga.net"}},
		{URLs: []string{"stun:stun.fwdnet.net"}},
		{URLs: []string{"stun:stun01.sipphone.com"}},
		{URLs: []string{"stun:stun.ideasip.com"}},
		{URLs: []string{"stun:stun.iptel.org"}},
		{URLs: []string{"stun:stun.rixtelecom.se"}},
		{URLs: []string{"stun:stun.schlund.de"}},
		{URLs: []string{"stun:stunserver.org"}},
		{URLs: []string{"stun:stun.softjoys.com"}},
		{URLs: []string{"stun:stun.voiparound.com"}},
		{URLs: []string{"stun:stun.voipbuster.com"}},
		{URLs: []string{"stun:stun.voipstunt.com"}},
		{URLs: []string{"stun:stun.voxgratia.org"}},
		{URLs: []string{"stun:stun.xten.com"}},
		{URLs: []string{"stun:stun.rtc.yandex.net"}},
	}
}

func (h *Handler) Bootstrap(w http.ResponseWriter, r *http.Request) {
	peerID := r.Header.Get("X-Peer-ID")
	if peerID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "X-Peer-ID header required"})
		return
	}

	var offer pion.SessionDescription
	if err := json.NewDecoder(r.Body).Decode(&offer); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid offer"})
		return
	}

	answer, err := h.acceptConnection(peerID, offer)
	if err != nil {
		log.Printf("webrtc accept %s: %v", peerID, err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	writeJSON(w, http.StatusOK, answer)
}

func (h *Handler) acceptConnection(peerID string, offer pion.SessionDescription) (*pion.SessionDescription, error) {
	h.closePeer(peerID)

	pc, err := pion.NewPeerConnection(h.config)
	if err != nil {
		return nil, fmt.Errorf("new pc: %w", err)
	}

	pc.OnDataChannel(func(dc *pion.DataChannel) {
		p := &peerConn{pc: pc, dc: dc, peerID: peerID}

		h.mu.Lock()
		h.peers[peerID] = p
		h.mu.Unlock()

		dc.OnMessage(func(msg pion.DataChannelMessage) {
			h.handleMessage(peerID, msg.Data)
		})

		dc.OnOpen(func() {
			log.Printf("[webrtc] peer %s connected", peerID)
		})
	})

	pc.OnConnectionStateChange(func(s pion.PeerConnectionState) {
		if s == pion.PeerConnectionStateDisconnected ||
			s == pion.PeerConnectionStateFailed ||
			s == pion.PeerConnectionStateClosed {
			h.closePeer(peerID)
			log.Printf("[webrtc] peer %s disconnected (%s)", peerID, s)
		}
	})

	if err := pc.SetRemoteDescription(offer); err != nil {
		pc.Close()
		return nil, fmt.Errorf("set remote desc: %w", err)
	}

	answer, err := pc.CreateAnswer(nil)
	if err != nil {
		pc.Close()
		return nil, fmt.Errorf("create answer: %w", err)
	}

	if err := pc.SetLocalDescription(answer); err != nil {
		pc.Close()
		return nil, fmt.Errorf("set local desc: %w", err)
	}

	<-pion.GatheringCompletePromise(pc)
	return pc.LocalDescription(), nil
}

func (h *Handler) handleMessage(peerID string, data []byte) {
	var req rpcRequest
	if err := json.Unmarshal(data, &req); err != nil || req.ID == "" || req.Method == "" {
		return
	}

	switch req.Method {
	case rpcMethodSignalRelay:
		h.handleSignalRelay(peerID, req)
	case rpcMethodConnectToPeer:
		h.handleConnectToPeer(peerID, req)
	default:
		h.sendError(peerID, req.ID, fmt.Sprintf("unknown method: %s", req.Method))
	}
}

func (h *Handler) handleSignalRelay(fromPeerID string, req rpcRequest) {
	var params SignalRelayRequest
	if err := json.Unmarshal(req.Params, &params); err != nil {
		h.sendError(fromPeerID, req.ID, "invalid signal_relay params")
		return
	}

	if h.sendNotification(params.TargetID, rpcNotifyIncomingSignal, IncomingSignal{
		From: params.From,
		Type: params.Type,
		Data: params.Data,
	}) {
		h.sendResult(fromPeerID, req.ID)
		return
	}

	sig := model.Signal{From: params.From, Type: params.Type, Data: params.Data}
	if err := h.store.SetSignal(context.Background(), params.TargetID, sig); err != nil {
		h.sendError(fromPeerID, req.ID, fmt.Sprintf("store signal: %v", err))
		return
	}

	h.sendResult(fromPeerID, req.ID)
}

func (h *Handler) handleConnectToPeer(fromPeerID string, req rpcRequest) {
	var params ConnectToPeerRequest
	if err := json.Unmarshal(req.Params, &params); err != nil {
		h.sendError(fromPeerID, req.ID, "invalid connect_to_peer params")
		return
	}

	if h.sendNotification(params.TargetID, rpcNotifyIncomingSignal, IncomingSignal{
		From: fromPeerID,
		Type: "offer",
		Data: params.Offer,
	}) {
		h.sendResult(fromPeerID, req.ID)
		return
	}

	sig := model.Signal{From: fromPeerID, Type: "offer", Data: params.Offer}
	if err := h.store.SetSignal(context.Background(), params.TargetID, sig); err != nil {
		h.sendError(fromPeerID, req.ID, fmt.Sprintf("store signal: %v", err))
		return
	}

	h.sendResult(fromPeerID, req.ID)
}

func (h *Handler) sendNotification(peerID, method string, params any) bool {
	h.mu.RLock()
	p, ok := h.peers[peerID]
	h.mu.RUnlock()

	if !ok || p.dc == nil {
		return false
	}

	raw, _ := json.Marshal(params)
	notif := rpcNotification{Method: method, Params: raw}
	data, _ := json.Marshal(notif)

	if err := p.dc.Send(data); err != nil {
		log.Printf("[webrtc] send notification to %s: %v", peerID, err)
		return false
	}

	return true
}

func (h *Handler) sendResult(peerID, reqID string) {
	h.mu.RLock()
	p, ok := h.peers[peerID]
	h.mu.RUnlock()

	if !ok || p.dc == nil {
		return
	}

	resp := rpcResponse{ID: reqID, Result: json.RawMessage("{}")}
	data, _ := json.Marshal(resp)
	p.dc.Send(data)
}

func (h *Handler) sendError(peerID, reqID, errMsg string) {
	h.mu.RLock()
	p, ok := h.peers[peerID]
	h.mu.RUnlock()

	if !ok || p.dc == nil {
		return
	}

	resp := rpcResponse{ID: reqID, Error: errMsg}
	data, _ := json.Marshal(resp)
	p.dc.Send(data)
}

func (h *Handler) closePeer(peerID string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if p, ok := h.peers[peerID]; ok {
		p.pc.Close()
		delete(h.peers, peerID)
	}
}

func (h *Handler) CloseAll() {
	h.mu.Lock()
	defer h.mu.Unlock()

	for id, p := range h.peers {
		p.pc.Close()
		delete(h.peers, id)
	}
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}
