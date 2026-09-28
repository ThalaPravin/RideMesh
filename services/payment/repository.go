package main

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrPaymentNotFound = errors.New("payment not found")
)

// PaymentRecord represents the PostgreSQL payment database row
type PaymentRecord struct {
	ID            string    `db:"id"`
	TripID        string    `db:"trip_id"`
	UserID        string    `db:"user_id"`
	Amount        float64   `db:"amount"`
	Status        string    `db:"status"`
	TransactionID string    `db:"transaction_id"`
	CreatedAt     time.Time `db:"created_at"`
}

// PaymentRepository handles database queries for payments
type PaymentRepository struct {
	db *pgxpool.Pool
}

// NewPaymentRepository creates a new PaymentRepository instance
func NewPaymentRepository(db *pgxpool.Pool) *PaymentRepository {
	return &PaymentRepository{db: db}
}

// CreatePayment inserts a new payment transaction record
func (r *PaymentRepository) CreatePayment(ctx context.Context, tripID, userID string, amount float64, status string) (*PaymentRecord, error) {
	txnID := "txn_" + uuid.New().String()
	query := `
		INSERT INTO payments (trip_id, user_id, amount, status, transaction_id)
		VALUES ($1, $2, $3, $4, $5)
		RETURNING id, trip_id, user_id, amount, status, transaction_id, created_at
	`
	p := &PaymentRecord{}
	err := r.db.QueryRow(ctx, query, tripID, userID, amount, status, txnID).Scan(
		&p.ID, &p.TripID, &p.UserID, &p.Amount, &p.Status, &p.TransactionID, &p.CreatedAt,
	)
	if err != nil {
		return nil, err
	}
	return p, nil
}

// GetPaymentByTransactionID fetches payment receipt details by transaction ID
func (r *PaymentRepository) GetPaymentByTransactionID(ctx context.Context, txnID string) (*PaymentRecord, error) {
	query := `
		SELECT id, trip_id, user_id, amount, status, transaction_id, created_at
		FROM payments
		WHERE transaction_id = $1
	`
	p := &PaymentRecord{}
	err := r.db.QueryRow(ctx, query, txnID).Scan(
		&p.ID, &p.TripID, &p.UserID, &p.Amount, &p.Status, &p.TransactionID, &p.CreatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrPaymentNotFound
		}
		return nil, err
	}
	return p, nil
}
