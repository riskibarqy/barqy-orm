package redis

import (
	"fmt"

	"github.com/go-redis/redis/v8"
)

// Insert inserts a key-value pair into Redis.
func Insert(key string, value interface{}) error {
	err := client.Set(ctx, key, value, 0).Err()
	if err != nil {
		return fmt.Errorf("failed to insert into Redis: %w", err)
	}
	return nil
}

// Find retrieves the value associated with a key in Redis.
func Find(key string) (string, error) {
	val, err := client.Get(ctx, key).Result()
	if err != nil {
		if err == redis.Nil {
			return "", fmt.Errorf("key does not exist")
		}
		return "", fmt.Errorf("failed to find in Redis: %w", err)
	}
	return val, nil
}

// Delete deletes a key from Redis.
func Delete(key string) error {
	err := client.Del(ctx, key).Err()
	if err != nil {
		return fmt.Errorf("failed to delete from Redis: %w", err)
	}
	return nil
}
