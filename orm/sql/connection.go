package sql

import (
	"database/sql"
	"fmt"
)

var db *sql.DB

// Connect initializes the DB connection based on the driver name and DSN.
func Connect(driverName, dsn string) error {
	var err error

	// Check if the provided driverName is supported before opening the connection.
	switch driverName {
	case "mysql", "postgres", "sqlite3", "mssql":
		// If a valid driver name is passed, open the connection
		db, err = sql.Open(driverName, dsn)
		if err != nil {
			return fmt.Errorf("failed to open database connection: %w", err)
		}
	default:
		return fmt.Errorf("unsupported driver %s", driverName)
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
