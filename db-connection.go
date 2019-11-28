package sqlbuilder

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// DBConnectionURL is just a struct to have a data source as a structure and not just a string
type DBConnectionURL struct {
	Driver   string
	Host     string
	Port     int
	DbName   string
	Username string
	Password string
	Params   string
}

// ToString converts DBConnectionURL struct to equivalent data source string
func (dbconn *DBConnectionURL) ToString() string {
	return fmt.Sprintf("%s://%s:%s@%s:%d/%s?%s", dbconn.Driver, dbconn.Username, dbconn.Password, dbconn.Host, dbconn.Port, dbconn.DbName, dbconn.Params)
}

// ParsePostgresqlURL parse a postrgesql data source string to DBConnectionURL
func ParsePostgresqlURL(url string) *DBConnectionURL {
	urlRegex, _ := regexp.Compile(`postgres://(\w+):(\S+)@(\S+):(\d+)/(\S+)\?sslmode=([a-zA-Z0-9_-]+)`)
	if !urlRegex.MatchString(url) {
		return nil
	}
	var db DBConnectionURL
	db.Driver = "postgres"
	rest := url[11:]
	pos := strings.Index(rest, ":")
	db.Username = rest[0:pos]
	rest = rest[pos:]
	pos = strings.LastIndex(rest, "@")
	db.Password = rest[1:pos]
	rest = rest[pos:]
	pos = strings.Index(rest, ":")
	db.Host = rest[1:pos]
	rest = rest[pos:]
	pos = strings.Index(rest, "/")
	db.Port, _ = strconv.Atoi(rest[1:pos])
	rest = rest[pos:]
	pos = strings.Index(rest, "?")
	db.DbName = rest[1:pos]
	db.Params = rest[pos+1:]
	return &db
}
