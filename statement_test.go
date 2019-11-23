package sqlbuilder_test

import (
	"sqlbuilder"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNewBuilder(t *testing.T) {
	sqlbuilder.SetDialect(sqlbuilder.DefaultDialect)
	stmt := sqlbuilder.New("SELECT *").From("table")
	defer stmt.Close()
	sql := stmt.String()
	args := stmt.Args()
	assert.Equal(t, "SELECT * FROM table", sql)
	assert.Empty(t, args)
}

func TestPgPlaceholders(t *testing.T) {
	sqlbuilder.SetDialect(sqlbuilder.PostgreSQL)
	stmt := sqlbuilder.From("series").
		Select("id").
		Where("time > ?", time.Now().Add(time.Hour*-24*14)).
		Where("(time < ?)", time.Now().Add(time.Hour*-24*7))
	defer stmt.Close()
	sql, _ := stmt.String(), stmt.Args()
	assert.Equal(t, "SELECT id FROM series WHERE time > $1 AND (time < $2)", sql)
}

func TestPgPlaceholderEscape(t *testing.T) {
	sqlbuilder.SetDialect(sqlbuilder.PostgreSQL)
	stmt := sqlbuilder.From("series").
		Select("id").
		Where("time \\?> ? + 1", time.Now().Add(time.Hour*-24*14)).
		Where("time < ?", time.Now().Add(time.Hour*-24*7))
	defer stmt.Close()
	sql, _ := stmt.String(), stmt.Args()
	assert.Equal(t, "SELECT id FROM series WHERE time ?> $1 + 1 AND time < $2", sql)
}
