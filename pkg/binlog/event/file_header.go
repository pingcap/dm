package event

import (
	"bytes"
	"time"

	"github.com/pingcap/errors"
	gmysql "github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"

	"github.com/pingcap/dm/pkg/gtid"
)

// GenCommonFileHeader generates a common binlog file header.
// for MySQL:
//   1. BinLogFileHeader, [ fe `bin` ]
//   2. FormatDescriptionEvent
//   3. PreviousGTIDsEvent
// for MariaDB:
//   1. BinLogFileHeader, [ fe `bin` ]
//   2. FormatDescriptionEvent
//   3. MariadbGTIDListEvent
//   -. MariadbBinlogCheckPointEvent, not added yet
func GenCommonFileHeader(flavor string, serverID uint32, gSet gtid.Set) ([]*replication.BinlogEvent, []byte, error) {
	var (
		header = &replication.EventHeader{
			Timestamp: uint32(time.Now().Unix()),
			ServerID:  serverID,
			Flags:     defaultHeaderFlags,
		}
		latestPos       = uint32(len(replication.BinLogFileHeader))
		prevGTIDsEv     *replication.BinlogEvent // for MySQL, this will be nil
		prevGTIDsEvData []byte
	)

	formatDescEv, err := GenFormatDescriptionEvent(header, latestPos)
	if err != nil {
		return nil, nil, errors.Annotatef(err, "generate FormatDescriptionEvent")
	}
	latestPos += uint32(len(formatDescEv.RawData)) // update latestPos

	switch flavor {
	case gmysql.MySQLFlavor:
		prevGTIDsEvData, err = GenPreviousGTIDsEvent(header, latestPos, gSet)
	case gmysql.MariaDBFlavor:
		prevGTIDsEv, err = GenMariaDBGTIDListEvent(header, latestPos, gSet)
		if err == nil {
			prevGTIDsEvData = prevGTIDsEv.RawData
		}
	default:
		return nil, nil, errors.NotSupportedf("flavor %s", flavor)
	}
	if err != nil {
		return nil, nil, errors.Annotatef(err, "generate PreviousGTIDsEvent/MariadbGTIDListEvent")
	}

	var buf bytes.Buffer
	_, err = buf.Write(replication.BinLogFileHeader)
	if err != nil {
		return nil, nil, errors.Annotatef(err, "write binlog file header % X", replication.BinLogFileHeader)
	}
	_, err = buf.Write(formatDescEv.RawData)
	if err != nil {
		return nil, nil, errors.Annotatef(err, "write FormatDescriptionEvent % X", formatDescEv.RawData)
	}
	_, err = buf.Write(prevGTIDsEvData)
	if err != nil {
		return nil, nil, errors.Annotatef(err, "write PreviousGTIDsEvent/MariadbGTIDListEvent % X", prevGTIDsEvData)
	}

	events := []*replication.BinlogEvent{formatDescEv, prevGTIDsEv}
	return events, buf.Bytes(), nil
}
