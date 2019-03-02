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

// GenCommonGTIDEvent generates a common GTID event.
func GenCommonGTIDEvent(flavor string, serverID uint32, latestPos uint32, gSet gtid.Set) (*replication.BinlogEvent, error) {
	if gSet == nil || len(gSet.String()) == 0 {
		return nil, errors.NotValidf("empty GTID set")
	}

	origin := gSet.Origin()
	if origin == nil {
		return nil, errors.NotValidf("GTID set string %s for MySQL", gSet)
	}

	var (
		header = &replication.EventHeader{
			Timestamp: uint32(time.Now().Unix()),
			ServerID:  serverID,
			Flags:     defaultHeaderFlags,
		}
		gtidEv *replication.BinlogEvent
		err    error
	)

	switch flavor {
	case gmysql.MySQLFlavor:
		mysqlGTIDs, ok := origin.(*gmysql.MysqlGTIDSet)
		if !ok {
			return nil, errors.NotValidf("GTID set string %s for MySQL", gSet)
		}
		if len(mysqlGTIDs.Sets) != 1 {
			return nil, errors.Errorf("only one GTID in set is supported, but got %d (%s)", len(mysqlGTIDs.Sets), gSet)
		}
		var uuidSet *gmysql.UUIDSet
		for _, uuidSet = range mysqlGTIDs.Sets {
		}
		intervals := uuidSet.Intervals
		if intervals.Len() != 1 {
			return nil, errors.Errorf("only one Interval in UUIDSet is supported, but got %d (%s)", intervals.Len(), gSet)
		}
		interval := intervals[0]
		if interval.Stop != interval.Start+1 {
			return nil, errors.Errorf("Interval's Stop should equal to Start+1, but got %+v (%s)", interval, gSet)
		}
		gtidEv, err = GenGTIDEvent(header, latestPos, defaultGTIDFlags, uuidSet.SID.String(), interval.Start, defaultLastCommitted, defaultSequenceNumber)
	case gmysql.MariaDBFlavor:
		mariaGTIDs, ok := origin.(*gmysql.MariadbGTIDSet)
		if !ok {
			return nil, errors.NotValidf("GTID set string %s for MariaDB", gSet)
		}
		if len(mariaGTIDs.Sets) != 1 {
			return nil, errors.Errorf("only one GTID in set is supported, but got %d (%s)", len(mariaGTIDs.Sets), gSet)
		}
		var mariaGTID *gmysql.MariadbGTID
		for _, mariaGTID = range mariaGTIDs.Sets {
		}
		if mariaGTID.ServerID != header.ServerID {
			return nil, errors.Errorf("server_id mismatch, in GTID (%d), in event header (%d)", mariaGTID.ServerID, header.ServerID)
		}
		gtidEv, err = GenMariaDBGTIDEvent(header, latestPos, mariaGTID.SequenceNumber, mariaGTID.DomainID)
		// in go-mysql, set ServerID in parseEvent. we try to set it directly
		gtidEvBody := gtidEv.Event.(*replication.MariadbGTIDEvent)
		gtidEvBody.GTID.ServerID = header.ServerID
	default:
		err = errors.NotValidf("GTID set %s with flavor %s", gSet, flavor)
	}
	return gtidEv, errors.Annotatef(err, "generate GTIDEvent")
}
