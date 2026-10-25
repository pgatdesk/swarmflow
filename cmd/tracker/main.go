// Command tracker runs the SwarmFlow bootstrap tracker. It only hands out peer
// addresses; file data never passes through it.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/pgatdesk/swarmflow/internal/discovery"
)

func main() {
	listen := flag.String("listen", "127.0.0.1:7000", "HTTP listen address")
	ttl := flag.Duration("ttl", 90*time.Second, "forget peers that have not announced for this long")
	interval := flag.Duration("interval", 30*time.Second, "re-announce interval told to peers")
	flag.Parse()

	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	t := discovery.NewTracker(log)
	t.TTL, t.Interval = *ttl, *interval

	ln, err := net.Listen("tcp", *listen)
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
	srv := &http.Server{Handler: t.Handler(), ReadHeaderTimeout: 5 * time.Second}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	go func() {
		<-ctx.Done()
		sctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		srv.Shutdown(sctx)
	}()
	log.Info("tracker listening", "addr", ln.Addr().String())
	if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}
