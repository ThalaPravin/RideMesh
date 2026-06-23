package db

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"
)

const schemaSQL = `
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS users (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    email VARCHAR(255) UNIQUE NOT NULL,
    password VARCHAR(255) NOT NULL,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS drivers (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    phone_no VARCHAR(20) UNIQUE NOT NULL,
    vehicle_model VARCHAR(100) NOT NULL,
    vehicle_number VARCHAR(50) UNIQUE NOT NULL,
    vehicle_type VARCHAR(50) NOT NULL,
    is_online BOOLEAN NOT NULL DEFAULT FALSE,
    rating NUMERIC(3,2) NOT NULL DEFAULT 5.00,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS trips (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    driver_id UUID REFERENCES drivers(id) ON DELETE SET NULL,
    pickup_lat NUMERIC(9,6) NOT NULL,
    pickup_lon NUMERIC(9,6) NOT NULL,
    dest_lat NUMERIC(9,6) NOT NULL,
    dest_lon NUMERIC(9,6) NOT NULL,
    amount NUMERIC(10,2) NOT NULL,
    status VARCHAR(50) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    started_at TIMESTAMP,
    ended_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS payments (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    trip_id UUID NOT NULL REFERENCES trips(id) ON DELETE CASCADE,
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    amount NUMERIC(10,2) NOT NULL,
    status VARCHAR(50) NOT NULL,
    transaction_id VARCHAR(100) UNIQUE NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);
`

// Seed dummy drivers to PostgreSQL
const seedSQL = `
INSERT INTO drivers (id, first_name, last_name, phone_no, vehicle_model, vehicle_number, vehicle_type, is_online, rating)
VALUES 
('acb336c4-15f2-47f6-8080-d744173a1f3e', 'Goku', 'Son', '11111111', 'Honda CR-V', 'MH 01A B 4321', 'SUV', true, 4.90),
('82bdbe64-3d32-43a0-a707-e206c0af5bba', 'Vegeta', 'Briefs', '11111122', 'Audi A4', 'MH 01A B 4322', 'Luxury', true, 4.90),
('388de7d9-e7f1-4338-89e7-2792cf2a4f5f', 'Gohan', 'Son', '11111133', 'Datsun Go', 'MH 01A B 4323', 'Micro', true, 4.70),
('db1eaa9b-753e-4631-84d4-0388b24839bd', 'Trunks', 'Briefs', '11111144', 'Hyundai Eon', 'MH 01A B 4324', 'Micro', true, 4.70),
('e69aae35-fd91-45e2-ad70-75df2d96f402', 'Bulma', 'Briefs', '11111155', 'BMW X1', 'MH 01A B 4325', 'Luxury', true, 4.70)
ON CONFLICT (phone_no) DO NOTHING;
`

// InitSchema automatically runs database migrations and seeding
func InitSchema(ctx context.Context, pool *pgxpool.Pool, log *zap.Logger) error {
	log.Info("Running database schema migrations...")
	
	// Create tables
	if _, err := pool.Exec(ctx, schemaSQL); err != nil {
		log.Error("Database schema migration failed", zap.Error(err))
		return err
	}
	
	log.Info("Database tables verified/created successfully.")
	
	// Seed dummy drivers
	if _, err := pool.Exec(ctx, seedSQL); err != nil {
		log.Error("Database seeding failed", zap.Error(err))
		return err
	}
	
	log.Info("Database seeding verified/completed successfully.")
	return nil
}
