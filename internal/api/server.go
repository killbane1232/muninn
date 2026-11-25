package api

import (
	"context"
	"log"
	"net/http"
	"time"

	"github.com/killbane1232/muninn/internal/handler"
	"github.com/killbane1232/muninn/internal/mesh"
	"github.com/killbane1232/muninn/internal/store"
	"github.com/killbane1232/muninn/internal/webrtc"
)

type Server struct {
	*http.Server
	RTC    *webrtc.Handler
	Mesh   *mesh.Mesh
	cancel context.CancelFunc
	store  store.Store
	cfg    Config
}

type Config struct {
	Mesh            mesh.Config
	Addr            string
	ReadTimeout     time.Duration
	WriteTimeout    time.Duration
	ShutdownTimeout time.Duration
	PurgeInterval   time.Duration
}

func DefaultConfig() Config {
	return Config{
		Addr:            ":8080",
		ReadTimeout:     10 * time.Second,
		WriteTimeout:    10 * time.Second,
		ShutdownTimeout: 15 * time.Second,
		PurgeInterval:   1 * time.Hour,
	}
}

func NewServer(cfg Config, st store.Store) (*Server, error) {
	rtc := webrtc.NewHandler(st)
	mux := http.NewServeMux()
	var network *mesh.Mesh
	var directory store.Store = st
	if cfg.Mesh.Enabled {
		var err error
		network, err = mesh.New(cfg.Mesh, st, rtc.DeliverLocal)
		if err != nil {
			return nil, err
		}
		directory = network
		network.RegisterRoutes(mux)
		rtc.RouteSignal = network.SetSignal
		network.SignalConnected = rtc.IsConnected
		rtc.PollSignals = network.PollSignals
	}
	pb := &handler.Phonebook{Store: directory, SendSignal: rtc.DeliverLocal}
	if network != nil {
		pb.SendSignal = network.SetSignal
	}
	mux.HandleFunc("GET /health", pb.Health)
	mux.HandleFunc("GET /api/v1/peers", pb.List)
	mux.HandleFunc("POST /api/v1/peers", pb.Register)
	mux.HandleFunc("PUT /api/v1/peers", pb.Refresh)
	mux.HandleFunc("GET /api/v1/peers/{id}", pb.Get)
	mux.HandleFunc("GET /api/v1/keys/{login}", pb.GetByKey)
	mux.HandleFunc("GET /api/v1/peers/best/thick", pb.GetBestThickPeers)
	mux.HandleFunc("GET /api/v1/peers/best", pb.GetBestPeers)
	mux.HandleFunc("DELETE /api/v1/peers/{id}", pb.Delete)
	mux.HandleFunc("POST /api/v1/peers/{id}/heartbeat", pb.Heartbeat)
	mux.HandleFunc("POST /api/v1/peers/{id}/chunk-reports", pb.ReportChunk)
	mux.HandleFunc("POST /api/v1/chunks/confirm", pb.ConfirmChunk)
	mux.HandleFunc("POST /api/v1/chunks/confirm-batch", pb.ConfirmChunkBatch)
	mux.HandleFunc("PUT /api/v1/files/{file_id}/chunks/{index}", pb.RegisterChunk)
	mux.HandleFunc("POST /api/v1/files/{file_id}/chunks", pb.RegisterChunkBatch)
	mux.HandleFunc("GET /api/v1/files/{file_id}/chunks", pb.GetChunksByFileID)
	mux.HandleFunc("GET /api/v1/recipient/chunks", pb.GetChunksByRecipient)
	mux.HandleFunc("POST /api/v1/peers/{id}/signals", pb.SetSignal)
	mux.HandleFunc("GET /api/v1/peers/{id}/signals", pb.PollSignals)
	mux.HandleFunc("POST /api/v1/chunks/read", pb.ReadChunk)
	mux.HandleFunc("/api/v1/ws", rtc.HandleWebSocket)

	srv := &Server{
		Server: &http.Server{
			Addr:         cfg.Addr,
			Handler:      mux,
			ReadTimeout:  cfg.ReadTimeout,
			WriteTimeout: cfg.WriteTimeout,
		},
		RTC:   rtc,
		Mesh:  network,
		store: st,
		cfg:   cfg,
	}

	ctx, cancel := context.WithCancel(context.Background())
	srv.cancel = cancel
	if network != nil {
		network.Start(ctx)
	}
	srv.startChunkCleaner(ctx, cfg.PurgeInterval)
	return srv, nil
}

func (s *Server) startChunkCleaner(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		return
	}
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
			n, err := s.store.DeleteExpiredChunks(ctx)
			if err != nil {
				log.Printf("chunk cleanup: %v", err)
			} else if n > 0 {
				log.Printf("chunk cleanup: removed %d expired chunks", n)
			}
		}
	}()
}

func (s *Server) Shutdown(ctx context.Context) error {
	s.cancel()
	s.RTC.CloseAll()
	if s.Mesh != nil {
		s.Mesh.Close()
	}
	return s.Server.Shutdown(ctx)
}
