package metrics

import (
	"context"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

var (
	// TripsRequestedTotal tracks total ride requests initiated
	TripsRequestedTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "rideflow_trips_requested_total",
			Help: "Total number of trip requests created",
		},
		[]string{"vehicle_type"},
	)

	// TripsMatchedTotal tracks successful driver-passenger matches
	TripsMatchedTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "rideflow_trips_matched_total",
			Help: "Total number of trips successfully matched with a driver",
		},
	)

	// TripsCompletedTotal tracks trips reached completion
	TripsCompletedTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "rideflow_trips_completed_total",
			Help: "Total number of completed trips",
		},
	)

	// PaymentsProcessedTotal tracks payment events by status
	PaymentsProcessedTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "rideflow_payments_processed_total",
			Help: "Total number of payments processed",
		},
		[]string{"status"},
	)

	// MatchingDuration tracks geospatial driver discovery latency
	MatchingDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "rideflow_matching_duration_seconds",
			Help:    "Latency in seconds for matching driver via Redis Geo",
			Buckets: prometheus.DefBuckets,
		},
	)
)

func init() {
	prometheus.MustRegister(
		TripsRequestedTotal,
		TripsMatchedTotal,
		TripsCompletedTotal,
		PaymentsProcessedTotal,
		MatchingDuration,
	)
}

// StartMetricsServer starts an HTTP server serving Prometheus metrics and health check
func StartMetricsServer(lc fx.Lifecycle, log *zap.Logger, port string) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"UP","service":"RideFlow"}`))
	})

	server := &http.Server{
		Addr:    ":" + port,
		Handler: mux,
	}

	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			log.Info("Starting Prometheus Metrics HTTP Server", zap.String("port", port))
			go func() {
				if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
					log.Error("Metrics server error", zap.Error(err))
				}
			}()
			return nil
		},
		OnStop: func(ctx context.Context) error {
			log.Info("Shutting down Prometheus Metrics HTTP Server")
			return server.Shutdown(ctx)
		},
	})
}
