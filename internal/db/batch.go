package db

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// BatchItemError is returned when a single statement in a batch fails.
// Callers can use errors.As to recover the failing row index for diagnostics.
type BatchItemError struct {
	Index int
	Err   error
}

func (e *BatchItemError) Error() string {
	return fmt.Sprintf("db batch item %d: %v", e.Index, e.Err)
}

func (e *BatchItemError) Unwrap() error {
	return e.Err
}

// BatchExec runs sql once per args row (same statement, different parameters).
// sql is caller-owned and typically includes INSERT ... ON CONFLICT DO UPDATE / DO NOTHING,
// or a parameterized UPDATE. Empty rows is a no-op.
// On statement failure, returns *BatchItemError with the failing index.
//
// Note: without a surrounding transaction each statement auto-commits, so
// DEFERRABLE FKs are still checked per row. Use BatchExecTx when parent/child
// rows are inserted in one batch (e.g. tags.parent_tag_id).
func BatchExec(ctx context.Context, conn DBInterface, sql string, rows [][]any) error {
	if len(rows) == 0 {
		return nil
	}
	if conn == nil {
		return ErrNilDB
	}
	return sendBatch(ctx, conn, sql, rows)
}

// BatchExecTx runs the batch inside a single transaction so DEFERRABLE FKs
// (INITIALLY DEFERRED) are checked at COMMIT after all rows are applied.
func BatchExecTx(ctx context.Context, conn DBInterface, sql string, rows [][]any) error {
	if len(rows) == 0 {
		return nil
	}
	if conn == nil {
		return ErrNilDB
	}
	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("db batch begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if err := sendBatch(ctx, tx, sql, rows); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("db batch commit: %w", err)
	}
	return nil
}

// batchSender is satisfied by *pgxpool.Pool and pgx.Tx.
type batchSender interface {
	SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults
}

func sendBatch(ctx context.Context, conn batchSender, sql string, rows [][]any) error {
	b := &pgx.Batch{}
	for _, args := range rows {
		b.Queue(sql, args...)
	}

	br := conn.SendBatch(ctx, b)
	defer br.Close()

	for i := 0; i < b.Len(); i++ {
		if _, err := br.Exec(); err != nil {
			return &BatchItemError{Index: i, Err: err}
		}
	}
	if err := br.Close(); err != nil {
		return fmt.Errorf("db batch close: %w", err)
	}
	return nil
}
