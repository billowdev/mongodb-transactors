package mongodb

import (
	"context"
	"fmt"
	"log"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type txKey struct{}

// injectTx injects the session into the context
func InjectTx(ctx context.Context, session mongo.Session) context.Context {
	return context.WithValue(ctx, txKey{}, session)
}

// extractTx extracts the session from the context
func ExtractTx(ctx context.Context) mongo.Session {
	if session, ok := ctx.Value(txKey{}).(mongo.Session); ok {
		return session
	}
	return nil
}

func HelperExtractTx(ctx context.Context, client *mongo.Client) mongo.Session {
	session := ExtractTx(ctx)
	if session == nil {
		// Start a new session if none exists in the context
		var err error
		session, err = client.StartSession()
		if err != nil {
			log.Printf("Failed to start session: %v", err)
			return nil
		}
	}
	return session
}

type TransactorImpl struct {
	client *mongo.Client
}

// GetDatabaseConnection returns the MongoDB client connection.
func (d *TransactorImpl) GetDatabaseConnection() *mongo.Client {
	return d.client
}

// BeginTransaction starts a new transaction session.
func (d *TransactorImpl) BeginTransaction() (mongo.Session, error) {
	session, err := d.client.StartSession()
	if err != nil {
		return nil, fmt.Errorf("failed to start transaction session: %w", err)
	}
	session.StartTransaction()
	return session, nil
}

// RollbackTransaction rolls back the transaction if it was started.
func (d *TransactorImpl) RollbackTransaction(session mongo.Session) error {
	if session == nil {
		return nil // No session to rollback
	}
	return session.AbortTransaction(context.Background())
}

// CommitTransaction commits the transaction if it was started.
func (d *TransactorImpl) CommitTransaction(session mongo.Session) error {
	if session == nil {
		return nil // No session to commit
	}
	return session.CommitTransaction(context.Background())
}

// WithinTransaction runs the provided function within a transaction context.
// The transaction is automatically committed if the function completes successfully, or rolled back if an error occurs.
func (d *TransactorImpl) WithinTransaction(ctx context.Context, tFunc func(ctx context.Context) error) error {
	// Start a new transaction session
	session, err := d.BeginTransaction()
	if err != nil {
		return err
	}
	defer func() {
		if r := recover(); r != nil {
			_ = d.RollbackTransaction(session)
			panic(r) // Re-panic after rollback
		} else if err != nil {
			_ = d.RollbackTransaction(session)
		} else {
			if commitErr := d.CommitTransaction(session); commitErr != nil {
				log.Printf("failed to commit transaction: %v", commitErr)
				err = commitErr
			}
		}
	}()

	// Run the callback function with the transaction context
	err = tFunc(InjectTx(ctx, session))
	return err
}

// WithTransactionContextTimeout executes a function within a transaction with a specified timeout.
// The transaction is committed if successful, or rolled back if an error occurs or the context times out.
func (d *TransactorImpl) WithTransactionContextTimeout(ctx context.Context, timeout time.Duration, tFunc func(ctx context.Context) error) error {
	// Create a new context with timeout
	transactionCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	// Start a new transaction session
	session, err := d.BeginTransaction()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	defer func() {
		if err := recover(); err != nil {
			_ = d.RollbackTransaction(session)
			log.Printf("Transaction rolled back due to panic: %v", err)
		} else if transactionCtx.Err() != nil {
			_ = d.RollbackTransaction(session)
			log.Printf("Transaction rolled back due to context error: %v", transactionCtx.Err())
		} else {
			if commitErr := d.CommitTransaction(session); commitErr != nil {
				log.Printf("failed to commit transaction: %v", commitErr)
				err = commitErr
			}
		}
	}()

	// Run the callback function with the transaction context
	err = tFunc(InjectTx(transactionCtx, session))
	if err != nil {
		_ = d.RollbackTransaction(session) // Mark the transaction as needing a rollback
		return err
	}

	return nil
}

// IDatabaseTransactor defines the interface for database transactions.
type IDatabaseTransactor interface {
	GetDatabaseConnection() *mongo.Client
	WithinTransaction(context.Context, func(ctx context.Context) error) error
	WithTransactionContextTimeout(ctx context.Context, timeout time.Duration, tFunc func(ctx context.Context) error) error
	BeginTransaction() (mongo.Session, error)
	RollbackTransaction(session mongo.Session) error
	CommitTransaction(session mongo.Session) error
}

// NewTransactorRepo creates a new instance of TransactorImpl.
func NewTransactorRepo(client *mongo.Client) IDatabaseTransactor {
	return &TransactorImpl{client: client}
}
