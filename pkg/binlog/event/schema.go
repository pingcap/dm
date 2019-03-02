package event

import (
	"fmt"
	"time"

	"github.com/pingcap/errors"
	"github.com/siddontang/go-mysql/replication"
)

// GenCreateDatabase generates binlog events for `CREATE DATABASE`.
func GenCreateDatabase(serverID uint32, latestPos uint32, schema string) ([]*replication.BinlogEvent, []byte, error) {
	return genDatabaseEvents(serverID, latestPos, schema, "CREATE DATABASE `%s`")
}

// GenDropDatabase generates binlog events for `DROP DATABASE`.
func GenDropDatabase(serverID uint32, latestPos uint32, schema string) ([]*replication.BinlogEvent, []byte, error) {
	return genDatabaseEvents(serverID, latestPos, schema, "DROP DATABASE `%s`")
}

func genDatabaseEvents(serverID uint32, latestPos uint32, schema string, template string) ([]*replication.BinlogEvent, []byte, error) {
	header := &replication.EventHeader{
		Timestamp: uint32(time.Now().Unix()),
		ServerID:  serverID,
		Flags:     defaultHeaderFlags,
	}
	query := fmt.Sprintf(template, schema)
	ev, err := GenQueryEvent(header, latestPos, defaultSlaveProxyID, defaultExecutionTime, defaultErrorCode, defaultStatusVars, []byte(schema), []byte(query))
	if err != nil {
		return nil, nil, errors.Annotatef(err, "generate QueryEvent for schema %s, query %s", schema, query)
	}
	return []*replication.BinlogEvent{ev}, ev.RawData, nil
}
