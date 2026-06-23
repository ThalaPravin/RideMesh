package main

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrDriverNotFound      = errors.New("driver not found")
	ErrDriverAlreadyExists = errors.New("driver already exists")
)

// Driver represents the database driver record
type Driver struct {
	ID            string  `db:"id"`
	FirstName     string  `db:"first_name"`
	LastName      string  `db:"last_name"`
	PhoneNo       string  `db:"phone_no"`
	VehicleModel  string  `db:"vehicle_model"`
	VehicleNumber string  `db:"vehicle_number"`
	VehicleType   string  `db:"vehicle_type"`
	IsOnline      bool    `db:"is_online"`
	Rating        float64 `db:"rating"`
}

// DriverRepository handles database queries for drivers
type DriverRepository struct {
	db *pgxpool.Pool
}

// NewDriverRepository creates a new DriverRepository instance
func NewDriverRepository(db *pgxpool.Pool) *DriverRepository {
	return &DriverRepository{db: db}
}

// CreateDriver inserts a new driver record
func (r *DriverRepository) CreateDriver(ctx context.Context, firstName, lastName, phoneNo, vehicleModel, vehicleNumber, vehicleType string) (*Driver, error) {
	query := `
		INSERT INTO drivers (first_name, last_name, phone_no, vehicle_model, vehicle_number, vehicle_type)
		VALUES ($1, $2, $3, $4, $5, $6)
		RETURNING id, first_name, last_name, phone_no, vehicle_model, vehicle_number, vehicle_type, is_online, rating
	`
	d := &Driver{}
	err := r.db.QueryRow(ctx, query, firstName, lastName, phoneNo, vehicleModel, vehicleNumber, vehicleType).Scan(
		&d.ID, &d.FirstName, &d.LastName, &d.PhoneNo, &d.VehicleModel, &d.VehicleNumber, &d.VehicleType, &d.IsOnline, &d.Rating,
	)
	if err != nil {
		if err.Error() == "ERROR: duplicate key value violates unique constraint \"drivers_phone_no_key\" (SQLSTATE 23505)" ||
			err.Error() == "ERROR: duplicate key value violates unique constraint \"drivers_vehicle_number_key\" (SQLSTATE 23505)" {
			return nil, ErrDriverAlreadyExists
		}
		return nil, err
	}
	return d, nil
}

// UpdateDriverStatus toggles driver availability status
func (r *DriverRepository) UpdateDriverStatus(ctx context.Context, id string, isOnline bool) (*Driver, error) {
	query := `
		UPDATE drivers
		SET is_online = $2
		WHERE id = $1
		RETURNING id, first_name, last_name, phone_no, vehicle_model, vehicle_number, vehicle_type, is_online, rating
	`
	d := &Driver{}
	err := r.db.QueryRow(ctx, query, id, isOnline).Scan(
		&d.ID, &d.FirstName, &d.LastName, &d.PhoneNo, &d.VehicleModel, &d.VehicleNumber, &d.VehicleType, &d.IsOnline, &d.Rating,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrDriverNotFound
		}
		return nil, err
	}
	return d, nil
}

// GetDriverByID fetches driver details by ID
func (r *DriverRepository) GetDriverByID(ctx context.Context, id string) (*Driver, error) {
	query := `
		SELECT id, first_name, last_name, phone_no, vehicle_model, vehicle_number, vehicle_type, is_online, rating
		FROM drivers
		WHERE id = $1
	`
	d := &Driver{}
	err := r.db.QueryRow(ctx, query, id).Scan(
		&d.ID, &d.FirstName, &d.LastName, &d.PhoneNo, &d.VehicleModel, &d.VehicleNumber, &d.VehicleType, &d.IsOnline, &d.Rating,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrDriverNotFound
		}
		return nil, err
	}
	return d, nil
}
