package main

import (
	"context"
	"fmt"

	"github.com/pingcap/errors"
	"github.com/siddontang/go-mysql/client"
)

// registerSlave register a fake slave on the master.
func registerSlave(addr, username, password string, serverID uint32) (*client.Conn, error) {
	conn, err := client.Connect(addr, username, password, "", func(c *client.Conn) {
	})
	if err != nil {
		return nil, errors.Trace(err)
	}

	// takes as an indication that the client is checksum-aware.
	// ref https://dev.mysql.com/doc/refman/8.0/en/c-api-binary-log-functions.html.
	_, err = conn.Execute(`SET @master_binlog_checksum='NONE'`)
	if err != nil {
		return nil, errors.Annotate(err, `SET @master_binlog_checksum='NONE'`)
	}

	conn.ResetSequence()
	packet := registerSlaveCommand(username, password, serverID)
	err = conn.WritePacket(packet)
	if err != nil {
		return nil, errors.Annotatef(err, "write COM_REGISTER_SLAVE packet %v", packet)
	}

	_, err = conn.ReadOKPacket()
	if err != nil {
		return nil, errors.Annotate(err, "read OK packet")
	}

	return conn, nil
}

// startSync starts to sync binlog event from <name, pos>.
func startSync(conn *client.Conn, serverID uint32, name string, pos uint32) error {
	conn.ResetSequence()
	packet := dumpCommand(serverID, name, pos)
	err := conn.WritePacket(packet)
	return errors.Trace(err)
}

// killConn kills the connection to the master server.
func killConn(conn *client.Conn) error {
	query := fmt.Sprintf(`KILL %d`, conn.GetConnectionID())
	_, err := conn.Execute(query)
	return errors.Annotate(err, query)
}

// readEventsWithGoMySQL reads binlog events from the master server with `go-mysql` pkg.
func readEventsWithGoMySQL(ctx context.Context, conn *client.Conn) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		data, err := conn.ReadPacket()
		if err != nil {
			return errors.Trace(err)
		}

		switch data[0] {
		case 0x00:
			continue // count event
		case 0xff:
			return errors.New("read event fail")
		case 0xfe:
			// Refer http://dev.mysql.com/doc/internals/en/packet-EOF_Packet.html
			fmt.Println("receive EOF packet, retry ReadPacket")
			continue
		default:
			fmt.Printf("invalid stream header %c\n", data[0])
			continue
		}
	}
}
