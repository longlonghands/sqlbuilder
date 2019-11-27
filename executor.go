package sqlbuilder

import (
	"context"
	"database/sql"
)

// Executor performs SQL queries.
// It's an interface accepted by Query, QueryRow and Exec methods.
// Both sql.DB, sql.Conn and sql.Tx can be passed as executor.
type Executor interface {
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row
}
