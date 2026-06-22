package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	pion "github.com/pion/webrtc/v3"

	"github.com/killbane1232/muninn/internal/api"
	"github.com/killbane1232/muninn/internal/store"
	_ "github.com/jackc/pgx/v5/stdlib"
	_ "modernc.org/sqlite"
)

func main() {
	cfg := api.DefaultConfig()
	if v := os.Getenv("MUNINN_ADDR"); v != "" {
		cfg.Addr = v
	}
	if v := os.Getenv("MUNINN_PURGE_INTERVAL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			cfg.PurgeInterval = d
		}
	}

	var st store.Store
	driver := os.Getenv("MUNINN_STORE_DRIVER")
	dsn := os.Getenv("MUNINN_STORE_DSN")
	if driver != "postgres" && driver != "internal" {
		driver = "sqlite"
	}

	switch driver {
	case "sqlite", "postgres":
		var err error
		st, err = store.NewDB(driver, dsn)
		if err != nil {
			log.Fatalf("open %s store: %v", driver, err)
		}
		log.Printf("using %s store", driver)
	default:
		st = store.NewMemory()
		log.Printf("using in-memory store")
	}

	if p := os.Getenv("MUNINN_ICE_SERVERS_PATH"); p != "" {
		data, err := os.ReadFile(p)
		if err != nil {
			log.Printf("read ice servers file: %v", err)
		} else {
			var ice []pion.ICEServer
			if err := json.Unmarshal(data, &ice); err != nil {
				log.Printf("invalid ice servers file: %v", err)
			} else {
				cfg.ICEServers = ice
			}
		}
	}

	srv := api.NewServer(cfg, st)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		log.Printf("muninn phonebook listening on %s", cfg.Addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("server: %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("shutting down...")

	srv.RTC.CloseAll()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("shutdown: %v", err)
	}
}
