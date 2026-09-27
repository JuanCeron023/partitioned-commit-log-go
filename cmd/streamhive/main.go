// Command streamhive runs the StreamHive event broker with a live dashboard.
//
// StreamHive is a lightweight, self-hosted event log — think "Kafka's core
// ideas in one small Go binary". Services publish events to partitioned,
// ordered, durable topics and consume them in independent consumer groups,
// which is exactly what you need to decouple microservices (order.created ->
// inventory, email, analytics) without running a heavy Kafka cluster. It
// deploys as one container on AWS ECS/Fargate.
package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/portfolio/streamhive/internal/api"
	"github.com/portfolio/streamhive/internal/broker"
)

func main() {
	var (
		httpAddr  = flag.String("http", ":8082", "dashboard/API listen address")
		dataDir   = flag.String("data", "./data", "directory for partition logs")
		staticDir = flag.String("static", "web/static", "dashboard static dir")
	)
	flag.Parse()

	if err := os.MkdirAll(*dataDir, 0o755); err != nil {
		log.Fatalf("mkdir data: %v", err)
	}
	b := broker.New(*dataDir)
	defer b.Close()

	// Seed a demo topic so the dashboard has something to show immediately.
	_ = b.CreateTopic("order.events", 3)

	srv := api.New(b)
	httpSrv := &http.Server{Addr: *httpAddr, Handler: srv.Routes(*staticDir)}
	go func() {
		log.Printf("StreamHive dashboard on http://localhost%s", *httpAddr)
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("http: %v", err)
		}
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	log.Println("shutting down")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = httpSrv.Shutdown(ctx)
}
