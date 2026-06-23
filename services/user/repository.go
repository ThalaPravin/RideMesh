package main

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrUserNotFound      = errors.New("user not found")
	ErrUserAlreadyExists = errors.New("user already exists")
)

// User represents the database user record
type User struct {
	ID        string    `db:"id"`
	Email     string    `db:"email"`
	Password  string    `db:"password"`
	FirstName string    `db:"first_name"`
	LastName  string    `db:"last_name"`
	CreatedAt time.Time `db:"created_at"`
}

// UserRepository handles database transactions for Users
type UserRepository struct {
	db *pgxpool.Pool
}

// NewUserRepository creates a new UserRepository instance
func NewUserRepository(db *pgxpool.Pool) *UserRepository {
	return &UserRepository{db: db}
}

// CreateUser inserts a new user record into PostgreSQL
func (r *UserRepository) CreateUser(ctx context.Context, email, hashedPassword, firstName, lastName string) (*User, error) {
	query := `
		INSERT INTO users (email, password, first_name, last_name)
		VALUES ($1, $2, $3, $4)
		RETURNING id, email, password, first_name, last_name, created_at
	`
	u := &User{}
	err := r.db.QueryRow(ctx, query, email, hashedPassword, firstName, lastName).Scan(
		&u.ID, &u.Email, &u.Password, &u.FirstName, &u.LastName, &u.CreatedAt,
	)
	if err != nil {
		// PostgreSQL unique violation error code
		if err.Error() == "ERROR: duplicate key value violates unique constraint \"users_email_key\" (SQLSTATE 23505)" {
			return nil, ErrUserAlreadyExists
		}
		return nil, err
	}
	return u, nil
}

// GetUserByEmail queries a user record by email address
func (r *UserRepository) GetUserByEmail(ctx context.Context, email string) (*User, error) {
	query := `
		SELECT id, email, password, first_name, last_name, created_at
		FROM users
		WHERE email = $1
	`
	u := &User{}
	err := r.db.QueryRow(ctx, query, email).Scan(
		&u.ID, &u.Email, &u.Password, &u.FirstName, &u.LastName, &u.CreatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrUserNotFound
		}
		return nil, err
	}
	return u, nil
}

// GetUserByID queries a user record by UUID
func (r *UserRepository) GetUserByID(ctx context.Context, id string) (*User, error) {
	query := `
		SELECT id, email, password, first_name, last_name, created_at
		FROM users
		WHERE id = $1
	`
	u := &User{}
	err := r.db.QueryRow(ctx, query, id).Scan(
		&u.ID, &u.Email, &u.Password, &u.FirstName, &u.LastName, &u.CreatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrUserNotFound
		}
		return nil, err
	}
	return u, nil
}
