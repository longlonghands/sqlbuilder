package sqlbuilder

import (
	"context"
	"database/sql"
)

// SQLExecutor is an implementation for Executor interface using "database/sql"
// it is not necessary to use this implementation
// it's here to demonstrate a wrapper around sql.DB for logging purpose of something elese
type SQLExecutor struct {
	db        *sql.DB
	name      string
	enableLog bool
}

func ignoreErr(error) {
}

func (ss *SQLExecutor) IsLogEnabled() bool {
	return ss.enableLog
}

func (ss *SQLExecutor) EnableLog(param bool) {
	ss.enableLog = param
}

func (ss *SQLExecutor) DB() *sql.DB {
	return ss.db
}

func (ss *SQLExecutor) Close() {
	ignoreErr(ss.db.Close())
}

func (ss *SQLExecutor) Exec(query string, args ...interface{}) (sql.Result, error) {
	return ss.db.Exec(query, args...)
}

func (ss *SQLExecutor) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return ss.db.ExecContext(ctx, query, args...)
}

func (ss *SQLExecutor) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	return ss.db.QueryContext(ctx, query, args...)
}

func (ss *SQLExecutor) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	return ss.db.QueryRowContext(ctx, query, args...)
}

// CreateSQLExecutor create an instance of SQLExecutor
func CreateSQLExecutor(name string, db *sql.DB, enableLog bool) *SQLExecutor {
	return &SQLExecutor{db: db, name: name, enableLog: enableLog}
}
