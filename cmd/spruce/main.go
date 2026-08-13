package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/lajosnagyuk/spruce/internal/broker"
)

func main() {
	cfg := broker.DefaultConfig()
	addr := env("SPRUCE_ADDR", ":8080")
	cfg.CacheBytes = envInt64("SPRUCE_CACHE_BYTES", cfg.CacheBytes)
	cfg.MaxMessage = envInt64("SPRUCE_MAX_MESSAGE_BYTES", cfg.MaxMessage)
	cfg.QueueDepth = int(envInt64("SPRUCE_QUEUE_DEPTH", int64(cfg.QueueDepth)))
	cfg.DefaultTTL = envDuration("SPRUCE_DEFAULT_TTL", cfg.DefaultTTL)
	cfg.MaxTTL = envDuration("SPRUCE_MAX_TTL", cfg.MaxTTL)
	cfg.AckDeadline = envDuration("SPRUCE_ACK_DEADLINE", cfg.AckDeadline)
	cfg.ReplicationQueueBytes = envInt64("SPRUCE_REPLICATION_QUEUE_BYTES", cfg.ReplicationQueueBytes)
	cfg.ActionQueueBytes = envInt64("SPRUCE_ACTION_QUEUE_BYTES", cfg.ActionQueueBytes)
	cfg.MaxInflightBytes = envInt64("SPRUCE_MAX_INFLIGHT_BYTES", cfg.MaxInflightBytes)
	cfg.MaxSubscriberInflightBytes = envInt64("SPRUCE_MAX_SUBSCRIBER_INFLIGHT_BYTES", cfg.MaxSubscriberInflightBytes)
	cfg.MaxAttempts = int(envInt64("SPRUCE_MAX_ATTEMPTS", int64(cfg.MaxAttempts)))
	cfg.IdempotencyEntries = int(envInt64("SPRUCE_IDEMPOTENCY_ENTRIES", int64(cfg.IdempotencyEntries)))
	cfg.MaxConcurrentRequests = int(envInt64("SPRUCE_MAX_CONCURRENT_REQUESTS", int64(cfg.MaxConcurrentRequests)))
	cfg.MaxStreams = int(envInt64("SPRUCE_MAX_STREAMS", int64(cfg.MaxStreams)))
	cfg.MaxInternalRequests = int(envInt64("SPRUCE_MAX_INTERNAL_REQUESTS", int64(cfg.MaxInternalRequests)))
	cfg.PeerToken = os.Getenv("SPRUCE_PEER_TOKEN")
	cfg.ClusterID = os.Getenv("SPRUCE_CLUSTER_ID")
	cfg.PeerCAFile = os.Getenv("SPRUCE_PEER_CA_FILE")
	cfg.ClientToken = os.Getenv("SPRUCE_CLIENT_TOKEN")
	cfg.AdminToken = os.Getenv("SPRUCE_ADMIN_TOKEN")
	cfg.BasicUsername = os.Getenv("SPRUCE_BASIC_USERNAME")
	cfg.BasicPassword = os.Getenv("SPRUCE_BASIC_PASSWORD")
	cfg.AdminBasicUsername = os.Getenv("SPRUCE_ADMIN_BASIC_USERNAME")
	cfg.AdminBasicPassword = os.Getenv("SPRUCE_ADMIN_BASIC_PASSWORD")
	if (cfg.BasicUsername == "") != (cfg.BasicPassword == "") || (cfg.AdminBasicUsername == "") != (cfg.AdminBasicPassword == "") {
		fmt.Fprintln(os.Stderr, "basic authentication username and password must be configured together")
		os.Exit(2)
	}
	if peers := os.Getenv("SPRUCE_PEERS"); peers != "" {
		self := strings.TrimRight(os.Getenv("SPRUCE_SELF_URL"), "/")
		for _, peer := range strings.Split(peers, ",") {
			peer = strings.TrimRight(strings.TrimSpace(peer), "/")
			if peer != "" && peer != self {
				cfg.Peers = append(cfg.Peers, peer)
			}
		}
	}
	if cfg.CacheBytes <= 0 || cfg.MaxMessage <= 0 || cfg.QueueDepth <= 0 || cfg.DefaultTTL <= 0 || cfg.MaxTTL <= 0 || cfg.AckDeadline <= 0 || cfg.ReplicationQueueBytes <= 0 || cfg.ActionQueueBytes <= 0 || cfg.MaxInflightBytes <= 0 || cfg.MaxSubscriberInflightBytes <= 0 || cfg.MaxAttempts <= 0 || cfg.IdempotencyEntries <= 0 || cfg.MaxConcurrentRequests <= 0 || cfg.MaxStreams <= 0 || cfg.MaxInternalRequests <= 0 || cfg.DefaultTTL > cfg.MaxTTL {
		fmt.Fprintln(os.Stderr, "SPRUCE configuration limits must be positive and default TTL must not exceed max TTL")
		os.Exit(2)
	}
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	cfg.Logger = log
	b := broker.New(cfg)
	defer b.Close()
	srv := &http.Server{Addr: addr, Handler: b.Handler(), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, IdleTimeout: 75 * time.Second, MaxHeaderBytes: 16 << 10}
	serverErr := make(chan error, 1)
	go func() {
		log.Info("spruce listening", "address", addr, "peers", len(cfg.Peers))
		certFile, keyFile := os.Getenv("SPRUCE_TLS_CERT_FILE"), os.Getenv("SPRUCE_TLS_KEY_FILE")
		var err error
		if certFile != "" || keyFile != "" {
			if certFile == "" || keyFile == "" {
				serverErr <- errors.New("both SPRUCE_TLS_CERT_FILE and SPRUCE_TLS_KEY_FILE are required")
				return
			}
			err = srv.ListenAndServeTLS(certFile, keyFile)
		} else {
			err = srv.ListenAndServe()
		}
		if err != nil && err != http.ErrServerClosed {
			serverErr <- err
		}
	}()
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-ch:
	case err := <-serverErr:
		log.Error("server stopped", "error", err)
		os.Exit(1)
	}
	shutdown, done := time.NewTimer(10*time.Second), make(chan struct{})
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
		close(done)
	}()
	select {
	case <-done:
	case <-shutdown.C:
	}
}
func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
func envInt64(k string, d int64) int64 {
	raw := os.Getenv(k)
	if raw == "" {
		return d
	}
	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s must be an integer, got %q\n", k, raw)
		os.Exit(2)
	}
	return v
}
func envDuration(k string, d time.Duration) time.Duration {
	raw := os.Getenv(k)
	if raw == "" {
		return d
	}
	v, err := time.ParseDuration(raw)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s must be a duration, got %q\n", k, raw)
		os.Exit(2)
	}
	return v
}
