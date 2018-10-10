package filter

import "strings"

// mysql system schema
var systemSchemas = map[string]struct{}{
	"information_schema": {},
	"mysql":              {},
	"performance_schema": {},
	"sys":                {},
}

// IsSystemSchema judge schema is system shema or not
// case insensitive
func IsSystemSchema(schema string) bool {
	schema = strings.ToLower(schema)
	_, ok := systemSchemas[schema]
	return ok
}
