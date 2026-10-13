package auth

import (
	"testing"
)

func TestHashAndCheckPassword(t *testing.T) {
	password := "SecurePass123!"

	hash, err := HashPassword(password)
	if err != nil {
		t.Fatalf("Expected no error when hashing password, got: %v", err)
	}

	if hash == password {
		t.Fatal("Hash should not equal the plain text password")
	}

	if !CheckPasswordHash(password, hash) {
		t.Fatal("Password verification failed for correct password")
	}

	if CheckPasswordHash("WrongPassword", hash) {
		t.Fatal("Password verification succeeded for incorrect password")
	}
}

func TestGenerateAndValidateToken(t *testing.T) {
	secret := "test-secret-key-9999"
	userID := "usr_abc12345-uuid"

	token, expiresAt, err := GenerateToken(userID, secret)
	if err != nil {
		t.Fatalf("Failed to generate token: %v", err)
	}

	if token == "" {
		t.Fatal("Expected non-empty token string")
	}

	if expiresAt.IsZero() {
		t.Fatal("Expected non-zero expiration timestamp")
	}

	// Validate valid token
	parsedUserID, err := ValidateToken(token, secret)
	if err != nil {
		t.Fatalf("Expected valid token, got error: %v", err)
	}

	if parsedUserID != userID {
		t.Fatalf("Expected userID %s, got %s", userID, parsedUserID)
	}

	// Validate token with wrong secret
	_, err = ValidateToken(token, "wrong-secret-key")
	if err == nil {
		t.Fatal("Expected validation error for token with invalid secret")
	}

	// Validate malformed token
	_, err = ValidateToken("invalid.token.payload", secret)
	if err == nil {
		t.Fatal("Expected validation error for malformed token string")
	}
}
