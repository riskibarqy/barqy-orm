package sql

import (
	"database/sql"
	"fmt"

	_ "github.com/denisenkom/go-mssqldb" // SQL Server driver
	_ "github.com/go-sql-driver/mysql"   // MySQL driver (example)
	_ "github.com/lib/pq"                // PostgreSQL driver
	_ "github.com/mattn/go-sqlite3"      // SQLite driver
)

var db *sql.DB

// Connect initializes the DB connection based on the driver name and DSN.
func Connect(driverName, dsn string) error {
	var err error
	db, err = sql.Open(driverName, dsn)
	if err != nil {
		return fmt.Errorf("failed to open database connection: %w", err)
	}

	if err = db.Ping(); err != nil {
		return fmt.Errorf("failed to ping database: %w", err)
	}

	return nil
}

// GetDB returns the current DB instance.
func GetDB() *sql.DB {
	return db
}

// Close closes the database connection.
func Close() error {
	if db != nil {
		return db.Close()
	}
	return nil
}
