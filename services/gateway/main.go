package main

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"go.uber.org/fx"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/ThalaPravin/RideMesh/pkg/config"
	"github.com/ThalaPravin/RideMesh/pkg/logger"
	pb "github.com/ThalaPravin/RideMesh/proto"
)

// GatewayServer manages HTTP REST to gRPC translation
type GatewayServer struct {
	cfg            *config.Config
	log            *zap.Logger
	userClient     pb.UserServiceClient
	tripClient     pb.TripServiceClient
	matchingClient pb.MatchingServiceClient
}

// NewGatewayServer creates a new Gateway instance and connects gRPC client pools
func NewGatewayServer(lc fx.Lifecycle, cfg *config.Config, log *zap.Logger) (*GatewayServer, error) {
	opts := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}

	// Dial microservices
	userConn, err := grpc.Dial("localhost:50051", opts...)
	if err != nil {
		log.Warn("Could not dial user service directly (will retry in container)", zap.Error(err))
	}

	tripConn, err := grpc.Dial("localhost:50053", opts...)
	if err != nil {
		log.Warn("Could not dial trip service directly", zap.Error(err))
	}

	matchingConn, err := grpc.Dial("localhost:50054", opts...)
	if err != nil {
		log.Warn("Could not dial matching service directly", zap.Error(err))
	}

	gw := &GatewayServer{
		cfg:            cfg,
		log:            log,
		userClient:     pb.NewUserServiceClient(userConn),
		tripClient:     pb.NewTripServiceClient(tripConn),
		matchingClient: pb.NewMatchingServiceClient(matchingConn),
	}

	lc.Append(fx.Hook{
		OnStop: func(ctx context.Context) error {
			if userConn != nil {
				_ = userConn.Close()
			}
			if tripConn != nil {
				_ = tripConn.Close()
			}
			if matchingConn != nil {
				_ = matchingConn.Close()
			}
			return nil
		},
	})

	return gw, nil
}

// StartHTTPGateway starts the public HTTP server
func StartHTTPGateway(lc fx.Lifecycle, gw *GatewayServer, log *zap.Logger) {
	mux := http.NewServeMux()

	// Auth routes
	mux.HandleFunc("/api/v1/auth/register", gw.handleRegister)
	mux.HandleFunc("/api/v1/auth/login", gw.handleLogin)

	// Trip routes
	mux.HandleFunc("/api/v1/trips/book", gw.handleBookTrip)
	mux.HandleFunc("/api/v1/trips/", gw.handleGetTrip)

	// Driver location updates
	mux.HandleFunc("/api/v1/drivers/location", gw.handleUpdateLocation)

	// Health and liveness
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"UP","gateway":"RideFlow API Gateway"}`))
	})

	server := &http.Server{
		Addr:         ":8080",
		Handler:      corsMiddleware(mux),
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			log.Info("Starting RideFlow HTTP REST API Gateway on :8080")
			go func() {
				if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
					log.Error("API Gateway HTTP server encountered an error", zap.Error(err))
				}
			}()
			return nil
		},
		OnStop: func(ctx context.Context) error {
			log.Info("Gracefully shutting down RideFlow API Gateway...")
			return server.Shutdown(ctx)
		},
	})
}

func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// REST Handlers
func (gw *GatewayServer) handleRegister(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req pb.RegisterRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	resp, err := gw.userClient.Register(ctx, &req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func (gw *GatewayServer) handleLogin(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req pb.LoginRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	resp, err := gw.userClient.Login(ctx, &req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func (gw *GatewayServer) handleBookTrip(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req pb.CreateTripRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	resp, err := gw.tripClient.CreateTrip(ctx, &req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func (gw *GatewayServer) handleGetTrip(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	tripID := strings.TrimPrefix(r.URL.Path, "/api/v1/trips/")
	if tripID == "" {
		http.Error(w, "Trip ID required", http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	resp, err := gw.tripClient.GetTrip(ctx, &pb.GetTripRequest{TripId: tripID})
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func (gw *GatewayServer) handleUpdateLocation(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req pb.UpdateLocationRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	resp, err := gw.matchingClient.UpdateLocation(ctx, &req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func main() {
	app := fx.New(
		fx.Provide(
			config.LoadConfig,
			logger.NewLogger,
			NewGatewayServer,
		),
		fx.Invoke(StartHTTPGateway),
	)

	app.Run()
}
