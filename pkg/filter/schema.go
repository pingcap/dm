package filter

import "strings"

// DM heartbeat schema / table name
var (
	DMHeartbeatSchema = "dm_heartbeat"
	DMHeartbeatTable  = "heartbeat"
)

// mysql system schema
var systemSchemas = map[string]struct{}{
	"information_schema": {},
	"mysql":              {},
	"performance_schema": {},
	"sys":                {},
	DMHeartbeatSchema:    {}, // do not create table in it manually
}

// IsSystemSchema judge schema is system shema or not
// case insensitive
func IsSystemSchema(schema string) bool {
	schema = strings.ToLower(schema)
	_, ok := systemSchemas[schema]
	return ok
}
