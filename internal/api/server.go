package api

import (
	"net/http"
	"time"

	"github.com/killbane1232/muninn/internal/handler"
	"github.com/killbane1232/muninn/internal/store"
)

type Config struct {
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
		PurgeInterval:   30 * time.Second,
	}
}

func NewServer(cfg Config, st store.Store) *http.Server {
	pb := &handler.Phonebook{Store: st}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", pb.Health)
	mux.HandleFunc("GET /api/v1/peers", pb.List)
	mux.HandleFunc("POST /api/v1/peers", pb.Register)
	mux.HandleFunc("GET /api/v1/peers/{id}", pb.Get)
	mux.HandleFunc("GET /api/v1/peers/by-username/{username}", pb.GetByUsername)
	mux.HandleFunc("GET /api/v1/peers/best", pb.GetBestPeers)
	mux.HandleFunc("DELETE /api/v1/peers/{id}", pb.Delete)
	mux.HandleFunc("POST /api/v1/peers/{id}/heartbeat", pb.Heartbeat)
	mux.HandleFunc("POST /api/v1/peers/{id}/chunk-reports", pb.ReportChunk)
	mux.HandleFunc("PUT /api/v1/files/{file_id}/chunks/{index}", pb.RegisterChunk)

	return &http.Server{
		Addr:         cfg.Addr,
		Handler:      mux,
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
	}
}
