package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Cpotenzone/sentinel-v3/cache"
	"github.com/Cpotenzone/sentinel-v3/catalog"
	"github.com/Cpotenzone/sentinel-v3/pipeline"
	"github.com/Cpotenzone/sentinel-v3/retry"
	"github.com/Cpotenzone/sentinel-v3/tools"
	"github.com/mark3labs/mcp-go/server"
)

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	// Initialize components
	appCache := cache.New(1 * time.Hour)
	cat := catalog.Load()
	retryQueue := retry.NewQueue()
	pipe := pipeline.New(cat, appCache, retryQueue)

	// Start retry scheduler (cron-like background worker)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go retryQueue.StartScheduler(ctx, pipe)

	// Create MCP server
	s := server.NewMCPServer(
		"sentinel-v3",
		"3.0.0",
		server.WithToolCapabilities(true),
	)

	// Register tools
	tools.Register(s, pipe)

	// Create HTTP handler for Streamable HTTP transport
	httpHandler := server.NewStreamableHTTPServer(s)

	// Setup HTTP mux
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"status":"healthy","service":"sentinel-v3","version":"3.0.0","cache_size":%d,"retry_queue_size":%d}`,
			appCache.Size(), retryQueue.Size())
	})
	mux.Handle("/mcp", httpHandler)

	srv := &http.Server{
		Addr:         ":" + port,
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 900 * time.Second, // Long timeout for data-heavy queries
		IdleTimeout:  120 * time.Second,
	}

	// Graceful shutdown
	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh
		log.Println("Shutting down...")
		cancel()
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer shutdownCancel()
		srv.Shutdown(shutdownCtx)
	}()

	log.Printf("sentinel-v3 starting on :%s", port)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("Server failed: %v", err)
	}
}
