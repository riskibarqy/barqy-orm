package mongo

import (
	"fmt"
)

// Insert inserts data into a MongoDB collection.
func Insert(collection string, data interface{}) error {
	col := client.Database("your_database").Collection(collection)
	_, err := col.InsertOne(ctx, data)
	if err != nil {
		return fmt.Errorf("failed to insert into MongoDB: %w", err)
	}
	return nil
}

// Find retrieves data from a MongoDB collection.
func Find(collection string, query interface{}, result interface{}) error {
	col := client.Database("your_database").Collection(collection)
	cur, err := col.Find(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to find in MongoDB: %w", err)
	}
	defer func() {
		// Check if closing the cursor produces an error
		if err := cur.Close(ctx); err != nil {
			// Handle the close error if necessary
			fmt.Printf("Failed to close cursor: %v\n", err)
		}
	}()
	if err = cur.All(ctx, result); err != nil {
		return fmt.Errorf("failed to read MongoDB results: %w", err)
	}
	return nil
}

// Delete deletes records from a MongoDB collection.
func Delete(collection string, query interface{}) error {
	col := client.Database("your_database").Collection(collection)
	_, err := col.DeleteMany(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to delete from MongoDB: %w", err)
	}
	return nil
}
