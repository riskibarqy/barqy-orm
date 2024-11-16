package sql

import (
	"context"
	"fmt"
	"reflect"
	"strings"
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
	defer rows.Close()

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
