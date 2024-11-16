package redis

import (
	"context"
	"fmt"

	"github.com/go-redis/redis/v8"
)

var client *redis.Client
var ctx = context.Background()

// Connect establishes a connection to Redis.
func Connect(addr, password string, db int) error {
	client = redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
		DB:       db,
	})

	_, err := client.Ping(ctx).Result()
	if err != nil {
		return fmt.Errorf("failed to connect to Redis: %w", err)
	}
	return nil
}

// Close closes the Redis connection.
func Close() error {
	return client.Close()
}
