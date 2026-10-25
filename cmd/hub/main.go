// Command hub aggregates the monitoring APIs of the peers known to a tracker,
// for Prometheus service discovery and Grafana dashboards.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/pgatdesk/swarmflow/internal/hub"
	"github.com/pgatdesk/swarmflow/internal/netx"
)

func main() {
	tracker := flag.String("tracker", "http://127.0.0.1:7000", "tracker URL")
	listen := flag.String("listen", "127.0.0.1:7100", "HTTP listen address")
	poll := flag.Duration("poll", time.Second, "how often to poll the tracker and peers")
	flag.Parse()

	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	h := hub.New(*tracker, *poll, log)
	ln, err := netx.Listen(*listen)
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	go h.Run(ctx)
	srv := &http.Server{Handler: h.Handler(), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		sctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		srv.Shutdown(sctx)
	}()
	log.Info("hub listening", "addr", ln.Addr().String(), "tracker", *tracker)
	if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}
