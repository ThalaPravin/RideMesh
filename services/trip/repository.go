package main

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrTripNotFound = errors.New("trip not found")
)

// TripRecord represents the PostgreSQL trip database row
type TripRecord struct {
	ID          string         `db:"id"`
	UserID      string         `db:"user_id"`
	DriverID    sql.NullString `db:"driver_id"`
	PickupLat   float64        `db:"pickup_lat"`
	PickupLon   float64        `db:"pickup_lon"`
	DestLat     float64        `db:"dest_lat"`
	DestLon     float64        `db:"dest_lon"`
	Amount      float64        `db:"amount"`
	Status      string         `db:"status"`
	CreatedAt   time.Time      `db:"created_at"`
	StartedAt   sql.NullTime   `db:"started_at"`
	EndedAt     sql.NullTime   `db:"ended_at"`
}

// TripRepository handles database queries for trips
type TripRepository struct {
	db *pgxpool.Pool
}

// NewTripRepository creates a new TripRepository instance
func NewTripRepository(db *pgxpool.Pool) *TripRepository {
	return &TripRepository{db: db}
}

// CreateTrip inserts a new trip record into PostgreSQL in REQUESTED status
func (r *TripRepository) CreateTrip(ctx context.Context, userID string, pickupLat, pickupLon, destLat, destLon, amount float64) (*TripRecord, error) {
	query := `
		INSERT INTO trips (user_id, pickup_lat, pickup_lon, dest_lat, dest_lon, amount, status)
		VALUES ($1, $2, $3, $4, $5, $6, 'REQUESTED')
		RETURNING id, user_id, driver_id, pickup_lat, pickup_lon, dest_lat, dest_lon, amount, status, created_at, started_at, ended_at
	`
	t := &TripRecord{}
	err := r.db.QueryRow(ctx, query, userID, pickupLat, pickupLon, destLat, destLon, amount).Scan(
		&t.ID, &t.UserID, &t.DriverID, &t.PickupLat, &t.PickupLon, &t.DestLat, &t.DestLon, &t.Amount, &t.Status, &t.CreatedAt, &t.StartedAt, &t.EndedAt,
	)
	if err != nil {
		return nil, err
	}
	return t, nil
}

// GetTripByID fetches trip details by ID
func (r *TripRepository) GetTripByID(ctx context.Context, id string) (*TripRecord, error) {
	query := `
		SELECT id, user_id, driver_id, pickup_lat, pickup_lon, dest_lat, dest_lon, amount, status, created_at, started_at, ended_at
		FROM trips
		WHERE id = $1
	`
	t := &TripRecord{}
	err := r.db.QueryRow(ctx, query, id).Scan(
		&t.ID, &t.UserID, &t.DriverID, &t.PickupLat, &t.PickupLon, &t.DestLat, &t.DestLon, &t.Amount, &t.Status, &t.CreatedAt, &t.StartedAt, &t.EndedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrTripNotFound
		}
		return nil, err
	}
	return t, nil
}

// AssignDriver binds a matched driver to the trip and updates status to ASSIGNED
func (r *TripRepository) AssignDriver(ctx context.Context, tripID string, driverID string) (*TripRecord, error) {
	query := `
		UPDATE trips
		SET driver_id = $2, status = 'ASSIGNED'
		WHERE id = $1
		RETURNING id, user_id, driver_id, pickup_lat, pickup_lon, dest_lat, dest_lon, amount, status, created_at, started_at, ended_at
	`
	t := &TripRecord{}
	err := r.db.QueryRow(ctx, query, tripID, driverID).Scan(
		&t.ID, &t.UserID, &t.DriverID, &t.PickupLat, &t.PickupLon, &t.DestLat, &t.DestLon, &t.Amount, &t.Status, &t.CreatedAt, &t.StartedAt, &t.EndedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrTripNotFound
		}
		return nil, err
	}
	return t, nil
}

// UpdateTripStatus handles state transitions (ARRIVED, STARTED, COMPLETED, CANCELED)
func (r *TripRepository) UpdateTripStatus(ctx context.Context, tripID string, newStatus string) (*TripRecord, error) {
	var query string
	now := time.Now()

	switch newStatus {
	case "STARTED":
		query = `
			UPDATE trips
			SET status = $2, started_at = $3
			WHERE id = $1
			RETURNING id, user_id, driver_id, pickup_lat, pickup_lon, dest_lat, dest_lon, amount, status, created_at, started_at, ended_at
		`
		t := &TripRecord{}
		err := r.db.QueryRow(ctx, query, tripID, newStatus, now).Scan(
			&t.ID, &t.UserID, &t.DriverID, &t.PickupLat, &t.PickupLon, &t.DestLat, &t.DestLon, &t.Amount, &t.Status, &t.CreatedAt, &t.StartedAt, &t.EndedAt,
		)
		return t, err

	case "COMPLETED":
		query = `
			UPDATE trips
			SET status = $2, ended_at = $3
			WHERE id = $1
			RETURNING id, user_id, driver_id, pickup_lat, pickup_lon, dest_lat, dest_lon, amount, status, created_at, started_at, ended_at
		`
		t := &TripRecord{}
		err := r.db.QueryRow(ctx, query, tripID, newStatus, now).Scan(
			&t.ID, &t.UserID, &t.DriverID, &t.PickupLat, &t.PickupLon, &t.DestLat, &t.DestLon, &t.Amount, &t.Status, &t.CreatedAt, &t.StartedAt, &t.EndedAt,
		)
		return t, err

	default:
		query = `
			UPDATE trips
			SET status = $2
			WHERE id = $1
			RETURNING id, user_id, driver_id, pickup_lat, pickup_lon, dest_lat, dest_lon, amount, status, created_at, started_at, ended_at
		`
		t := &TripRecord{}
		err := r.db.QueryRow(ctx, query, tripID, newStatus).Scan(
			&t.ID, &t.UserID, &t.DriverID, &t.PickupLat, &t.PickupLon, &t.DestLat, &t.DestLon, &t.Amount, &t.Status, &t.CreatedAt, &t.StartedAt, &t.EndedAt,
		)
		return t, err
	}
}

// CancelTrip sets status to CANCELED
func (r *TripRepository) CancelTrip(ctx context.Context, tripID string) (*TripRecord, error) {
	return r.UpdateTripStatus(ctx, tripID, "CANCELED")
}
