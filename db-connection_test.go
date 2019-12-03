package sqlbuilder_test

import (
	"sqlbuilder"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParsePostgresqlURL(t *testing.T) {
	dbconn := sqlbuilder.ParsePostgresqlURL("postgres://postgres:password@localhost:5432/testdb1?sslmode=disable")
	assert.Equal(t, dbconn != nil, true)
	assert.Equal(t, dbconn.DbName, "testdb1")
	assert.Equal(t, dbconn.Host, "localhost")
	assert.Equal(t, dbconn.Port, 5432)
	assert.Equal(t, dbconn.Username, "postgres")
	assert.Equal(t, dbconn.Password, "password")

}
