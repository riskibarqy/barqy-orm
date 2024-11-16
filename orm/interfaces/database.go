package interfaces

// Database defines the common interface for all databases.
type Database interface {
	Connect() error
	Close() error
	Insert(table string, data interface{}) error
	Find(table string, query string, result interface{}) error
	Update(table string, query string, update string) error
	Delete(table string, query string) error
}
