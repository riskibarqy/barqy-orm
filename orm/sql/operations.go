package sql

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"time"
)

// Define constants for operators
const (
	OperatorEqual    = "="
	OperatorGreater  = ">"
	OperatorLessThan = "<"
	OperatorIn       = "IN"
	OperatorBetween  = "BETWEEN"
	OperatorLike     = "LIKE"
	OperatorNotIn    = "NOT IN"
	OperatorNotLike  = "NOT LIKE"
)

// QueryBuilder constructs dynamic SQL queries.
type QueryBuilder struct {
	Table           string
	Columns         []string
	WhereConditions []map[string]interface{}
	WhereOperators  []map[string]string // Store operators for each condition
	OrderByFields   []interface{}
	LimitCount      int
	CursorField     string      // Field used for cursor pagination
	CursorValue     interface{} // The cursor value for pagination
	Parameters      []interface{}
	Model           interface{} // The model (struct) for reflection
}

// NewQueryBuilder initializes a new query builder.
func NewQueryBuilder(table string, model interface{}) *QueryBuilder {
	return &QueryBuilder{
		Table:   table,
		Model:   model,
		Columns: []string{}, // Default to empty; we will generate columns using reflection
	}
}

// GetColumnsFromStruct reads the field names of a struct (exported fields only).
func (qb *QueryBuilder) GetColumnsFromStruct() []string {
	var columns []string
	t := reflect.TypeOf(qb.Model)

	// Check if the model is a pointer, and get the underlying struct
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	// Iterate over the struct fields and get the names of exported fields
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		// Only include exported fields (starting with uppercase letter)
		if field.PkgPath == "" {
			columns = append(columns, field.Name)
		}
	}

	return columns
}

// Select specifies the columns to select, or uses the struct's fields by default.
func (qb *QueryBuilder) Select(columns ...string) *QueryBuilder {
	if len(columns) > 0 {
		qb.Columns = columns
	} else if qb.Model != nil {
		// Default: Get columns from the struct if none are provided
		qb.Columns = qb.GetColumnsFromStruct()
	}
	return qb
}

// Where adds multiple dynamic WHERE conditions in the form of []map[string]interface{}
func (qb *QueryBuilder) Where(conditions []map[string]interface{}, operators []map[string]string) *QueryBuilder {
	qb.WhereConditions = conditions
	qb.WhereOperators = operators
	return qb
}

// OrderBy allows multiple sorting fields, accepting a slice of strings or maps (e.g., "field ASC", "field DESC")
func (qb *QueryBuilder) OrderBy(fields ...interface{}) *QueryBuilder {
	qb.OrderByFields = fields
	return qb
}

// Limit specifies the number of results to return.
func (qb *QueryBuilder) Limit(limit int) *QueryBuilder {
	qb.LimitCount = limit
	return qb
}

// Cursor sets the cursor for pagination (used instead of OFFSET).
func (qb *QueryBuilder) Cursor(field string, value interface{}) *QueryBuilder {
	qb.CursorField = field
	qb.CursorValue = value
	return qb
}

// Build constructs the final SQL query string.
func (qb *QueryBuilder) Build() (string, []interface{}) {
	query := fmt.Sprintf("SELECT %s FROM %s ", strings.Join(qb.Columns, ", "), qb.Table)

	// Handle WHERE conditions (if any)
	if len(qb.WhereConditions) > 0 {
		var whereClauses []string
		for i, condition := range qb.WhereConditions {
			for field, value := range condition {
				// Only process if the value is not nil or empty
				if value == nil {
					continue
				}

				// Get the operator for this condition, default to equality if none provided
				operator := OperatorEqual
				if len(qb.WhereOperators) > i {
					if op, ok := qb.WhereOperators[i][field]; ok {
						operator = op
					}
				}

				// Handle special operators (IN, LIKE, BETWEEN, etc.)
				var clause string
				switch operator {
				case OperatorIn:
					// For IN condition, the value should be a slice
					vals := value.([]interface{})
					placeholders := make([]string, len(vals))
					for i := range vals {
						placeholders[i] = "?"
					}
					clause = fmt.Sprintf("%s %s (%s)", field, OperatorIn, strings.Join(placeholders, ", "))
					qb.Parameters = append(qb.Parameters, vals...)
				case OperatorNotIn:
					// For NOT IN condition, the value should be a slice
					vals := value.([]interface{})
					placeholders := make([]string, len(vals))
					for i := range vals {
						placeholders[i] = "?"
					}
					clause = fmt.Sprintf("%s %s (%s)", field, OperatorNotIn, strings.Join(placeholders, ", "))
					qb.Parameters = append(qb.Parameters, vals...)
				case OperatorBetween:
					// BETWEEN expects two values
					vals := value.([]interface{})
					clause = fmt.Sprintf("%s %s ? AND ?", field, OperatorBetween)
					qb.Parameters = append(qb.Parameters, vals...)
				case OperatorLike:
					clause = fmt.Sprintf("%s %s ?", field, OperatorLike)
					qb.Parameters = append(qb.Parameters, value)
				default:
					// Default to equality operator
					clause = fmt.Sprintf("%s %s ?", field, operator)
					qb.Parameters = append(qb.Parameters, value)
				}

				whereClauses = append(whereClauses, clause)
			}
		}
		query += "WHERE " + strings.Join(whereClauses, " AND ")
	}

	// Handle ORDER BY conditions (if any)
	if len(qb.OrderByFields) > 0 {
		var orderByClauses []string
		for _, field := range qb.OrderByFields {
			switch v := field.(type) {
			case string:
				orderByClauses = append(orderByClauses, v)
			case map[string]string:
				for f, direction := range v {
					orderByClauses = append(orderByClauses, fmt.Sprintf("%s %s", f, direction))
				}
			}
		}
		query += " ORDER BY " + strings.Join(orderByClauses, ", ")
	}

	// Handle Cursor Pagination (if any)
	if qb.CursorField != "" && qb.CursorValue != nil {
		query += fmt.Sprintf(" AND %s > ? ", qb.CursorField)
		qb.Parameters = append(qb.Parameters, qb.CursorValue)
	}

	// Handle LIMIT (if any)
	if qb.LimitCount > 0 {
		query += fmt.Sprintf(" LIMIT %d", qb.LimitCount)
	}

	return query, qb.Parameters
}

// Execute runs the query and returns the results as []map[string]interface{}
func (qb *QueryBuilder) Execute(ctx context.Context) ([]map[string]interface{}, error) {
	query, params := qb.Build()

	// Execute the query and fetch rows
	rows, err := GetDB().QueryContext(ctx, query, params...)
	if err != nil {
		return nil, fmt.Errorf("query execution failed: %w", err)
	}

	defer func() {
		// Check if closing the rows produces an error
		if err := rows.Close(); err != nil {
			// Handle the close error if necessary
			fmt.Printf("Failed to close rows: %v\n", err)
		}
	}()

	// Read the rows into a slice of maps
	var results []map[string]interface{}
	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %w", err)
	}

	// Loop through each row and store the result as a map
	for rows.Next() {
		columnsData := make([]interface{}, len(columns))
		columnPointers := make([]interface{}, len(columns))
		for i := range columnsData {
			columnPointers[i] = &columnsData[i]
		}

		if err := rows.Scan(columnPointers...); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		rowMap := make(map[string]interface{})
		for i, column := range columns {
			rowMap[column] = columnsData[i]
		}
		results = append(results, rowMap)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("row iteration error: %w", err)
	}

	return results, nil
}

// Create inserts a single record into the database.
func Create(tableName string, data map[string]interface{}) error {
	// Prepare columns and values for insertion
	columns := []string{}
	values := []interface{}{}
	for col, val := range data {
		columns = append(columns, col)
		values = append(values, val)
	}

	// Build the SQL query
	placeholders := make([]string, len(data))
	for i := range placeholders {
		placeholders[i] = "?"
	}
	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", tableName, strings.Join(columns, ", "), strings.Join(placeholders, ", "))

	// Execute the query
	_, err := db.Exec(query, values...)
	if err != nil {
		return fmt.Errorf("failed to insert record: %w", err)
	}

	return nil
}

// CreateBulk inserts multiple records into the database.
func CreateBulk(tableName string, data []map[string]interface{}) error {
	if len(data) == 0 {
		return fmt.Errorf("no data provided for bulk insert")
	}

	columns := []string{}
	for col := range data[0] {
		columns = append(columns, col)
	}

	placeholders := []string{}
	values := []interface{}{}
	for _, row := range data {
		placeholdersRow := []string{}
		for _, col := range columns {
			placeholdersRow = append(placeholdersRow, "?")
			values = append(values, row[col])
		}
		placeholders = append(placeholders, fmt.Sprintf("(%s)", strings.Join(placeholdersRow, ", ")))
	}

	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s", tableName, strings.Join(columns, ", "), strings.Join(placeholders, ", "))

	_, err := db.Exec(query, values...)
	if err != nil {
		return fmt.Errorf("failed to insert bulk records: %w", err)
	}

	return nil
}

// Update updates an existing record in the database.
func Update(tableName string, data map[string]interface{}, where map[string]interface{}) error {
	// Build SET clause
	setClauses := []string{}
	args := []interface{}{}
	for col, val := range data {
		setClauses = append(setClauses, fmt.Sprintf("%s = ?", col))
		args = append(args, val)
	}

	// Build WHERE clause
	whereClauses := []string{}
	for col, val := range where {
		whereClauses = append(whereClauses, fmt.Sprintf("%s = ?", col))
		args = append(args, val)
	}

	// Combine everything into the final SQL query
	setStr := strings.Join(setClauses, ", ")
	whereStr := "WHERE " + strings.Join(whereClauses, " AND ")
	query := fmt.Sprintf("UPDATE %s SET %s %s", tableName, setStr, whereStr)

	// Execute the query
	_, err := db.Exec(query, args...)
	if err != nil {
		return fmt.Errorf("failed to update record: %w", err)
	}

	return nil
}

// Delete removes a record from the database.
func Delete(tableName string, where map[string]interface{}) error {
	// Build the WHERE clause
	whereClauses := []string{}
	args := []interface{}{}
	for col, val := range where {
		whereClauses = append(whereClauses, fmt.Sprintf("%s = ?", col))
		args = append(args, val)
	}

	whereStr := "WHERE " + strings.Join(whereClauses, " AND ")
	query := fmt.Sprintf("DELETE FROM %s %s", tableName, whereStr)

	_, err := db.Exec(query, args...)
	if err != nil {
		return fmt.Errorf("failed to delete record: %w", err)
	}

	return nil
}

// SoftDelete performs a "soft" delete by setting a "deleted_at" timestamp.
func SoftDelete(tableName string, where map[string]interface{}) error {
	// Build the WHERE clause
	whereClauses := []string{}
	args := []interface{}{}
	for col, val := range where {
		whereClauses = append(whereClauses, fmt.Sprintf("%s = ?", col))
		args = append(args, val)
	}

	// Add the current timestamp to the update data
	setStr := "deleted_at = ?"
	args = append(args, time.Now())

	whereStr := "WHERE " + strings.Join(whereClauses, " AND ")
	query := fmt.Sprintf("UPDATE %s SET %s %s", tableName, setStr, whereStr)

	_, err := db.Exec(query, args...)
	if err != nil {
		return fmt.Errorf("failed to perform soft delete: %w", err)
	}

	return nil
}

// UpdateBulk updates multiple records in bulk.
func UpdateBulk(tableName string, data []map[string]interface{}, where map[string]interface{}) error {
	if len(data) == 0 {
		return fmt.Errorf("no data provided for bulk update")
	}

	// Build the bulk update logic here...
	// For simplicity, we can loop over the data and call the Update method for each record.
	for _, row := range data {
		if err := Update(tableName, row, where); err != nil {
			return err
		}
	}

	return nil
}

// GetOrCreate attempts to find an existing record based on the provided conditions.
// If no record is found, it creates a new one.
func (qb *QueryBuilder) GetOrCreate(ctx context.Context, createData map[string]interface{}) (map[string]interface{}, error) {
	// Build the SELECT query based on the conditions provided
	selectQuery, selectParams := qb.Build()

	// Execute the SELECT query to check if the record exists
	rows, err := GetDB().QueryContext(ctx, selectQuery, selectParams...)
	if err != nil {
		return nil, fmt.Errorf("query execution failed: %w", err)
	}
	defer rows.Close()

	var result map[string]interface{}
	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %w", err)
	}

	// Check if a record was returned
	if rows.Next() {
		columnsData := make([]interface{}, len(columns))
		columnPointers := make([]interface{}, len(columns))
		for i := range columnsData {
			columnPointers[i] = &columnsData[i]
		}

		if err := rows.Scan(columnPointers...); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		result = make(map[string]interface{})
		for i, column := range columns {
			result[column] = columnsData[i]
		}

		return result, nil // Return the existing record if found
	}

	// If no record found, perform an INSERT operation to create it
	insertQuery, insertParams := qb.BuildInsertQuery(createData)
	_, err = GetDB().ExecContext(ctx, insertQuery, insertParams...)
	if err != nil {
		return nil, fmt.Errorf("failed to insert record: %w", err)
	}

	// Return the newly created record (optionally, you could fetch it again here)
	return createData, nil
}

// BuildInsertQuery creates an SQL insert query from the provided data map.
func (qb *QueryBuilder) BuildInsertQuery(data map[string]interface{}) (string, []interface{}) {
	var columns []string
	var placeholders []string
	var values []interface{}

	// Prepare columns and placeholders for the query
	for column, value := range data {
		columns = append(columns, column)
		placeholders = append(placeholders, "?")
		values = append(values, value)
	}

	// Build the INSERT query
	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", qb.Table, strings.Join(columns, ", "), strings.Join(placeholders, ", "))
	return query, values
}
