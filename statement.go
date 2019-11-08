package sqlbuilder

import "github.com/valyala/bytebufferpool"

/*
Statement provides a set of helper methods for SQL statement building and execution.
Use one of the following methods to create a SQL statement builder instance:
	sqlbuilder.From("table")
	sqlbuilder.Select("field")
	sqlbuilder.InsertInto("table")
	sqlbuilder.UpdateUser("table")
	sqlbuilder.DeleteFrom("table")
For other SQL statements use New:
	q := sqlbuilder.New("TRUNCATE")
	for _, table := range tablesToBeEmptied {
		q.Expr(table)
	}
	err := q.ExecAndClose(ctx, db)
	if err != nil {
		panic(err)
	}
*/
type Statement interface{}

type statement struct {
	dialect  Dialect
	position int
	parts    []statementPart
	buffer   *bytebufferpool.ByteBuffer
	sql      *bytebufferpool.ByteBuffer
	args     []interface{}
	dest     []interface{}
}

type statementPart struct {
	position int
	bufLow   int
	bufHigh  int
	hasExpr  bool
	argLen   int
}

func getBuffer() *bytebufferpool.ByteBuffer {
	return bytebufferpool.Get()
}

func putBuffer(buf *bytebufferpool.ByteBuffer) {
	bytebufferpool.Put(buf)
}

func newStmt() *statement {
	return &statement{
		parts: make([]statementPart, 0, 8),
	}
}

func getStmt(d Dialect) *statement {
	stmt := newStmt()
	stmt.dialect = d
	stmt.buffer = getBuffer()
	return stmt
}

func reuseStmt(q *statement) {
	q.parts = q.parts[:0]
	if len(q.args) > 0 {
		for n := range q.args {
			q.args[n] = nil
		}
		q.args = q.args[:0]
	}
	if len(q.dest) > 0 {
		for n := range q.dest {
			q.dest[n] = nil
		}
		q.dest = q.dest[:0]
	}
	putBuffer(q.buffer)
	q.buffer = nil
	if q.sql != nil {
		putBuffer(q.sql)
	}
	q.sql = nil

	// stmtPool.Put(q)
}

// addChunk adds a clause or expression to a statement.
func (q *statement) addChunk(pos int, clause, expr string, args []interface{}, sep string) (index int) {
	return index
}

var (
	space            = []byte{' '}
	placeholder      = []byte{'?'}
	placeholderComma = []byte{'?', ','}
	joinOn           = []byte{' ', 'O', 'N', ' ', '('}
)

const (
	_        = iota
	posStart = 100 * iota
	posWith
	posInsert
	posInsertFields
	posValues
	posDelete
	posUpdate
	posSet
	posSelect
	posInto
	posFrom
	posWhere
	posGroupBy
	posHaving
	posUnion
	posOrderBy
	posLimit
	posOffset
	posReturning
	posEnd
)

/*
New initializes a SQL statement builder instance with an arbitrary verb.
Use sqlbuilder.Select(), sqlbuilder.InsertInto(), sqlbuilder.DeleteFrom() to start
common SQL statements.
Use New for special cases like this:
	q := sqlbuilder.New("TRANCATE")
	for _, table := range tableNames {
		q.Expr(table)
	}
	q.Clause("RESTART IDENTITY")
	err := q.ExecAndClose(ctx, db)
	if err != nil {
		panic(err)
	}
*/
func New(verb string, args ...interface{}) Statement {
	return selectedDialect.New(verb, args...)
}
