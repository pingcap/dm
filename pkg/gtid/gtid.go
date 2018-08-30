// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package gtid

import (
	"github.com/juju/errors"
	"github.com/siddontang/go-mysql/mysql"
)

// Set provide gtid operations for syncer
type Set interface {
	Set(mysql.GTIDSet) error
	// compute set of self and other gtid set
	// 1. keep intersection of self and other gtid set
	// 2. keep complementary set of other gtid set except master identifications that not in self gtid
	// masters => master identification set, represents which db instances do write in one replicate group
	// example: self gtid set [xx:1-2, yy:1-3, xz:1-4], other gtid set [xx:1-4, yy:1-12, xy:1-3]. master ID set [xx]
	// => [xx:1-2, yy:1-3, xy:1-3]
	// more examples ref test cases
	Replace(other Set, masters []interface{}) error
	Clone() Set
	Origin() mysql.GTIDSet

	String() string
}

// ParserGTID parses GTID from string
func ParserGTID(flavor, gtidStr string) (Set, error) {
	var (
		m   Set
		err error
	)

	gtid, err := mysql.ParseGTIDSet(flavor, gtidStr)
	if err != nil {
		return nil, errors.Trace(err)
	}

	switch flavor {
	case mysql.MariaDBFlavor:
		m = &mariadbGTIDSet{}
	case mysql.MySQLFlavor:
		m = &mySQLGTIDSet{}
	default:
		return nil, errors.NotSupportedf("flavor %s and gtid %s", flavor, gtidStr)
	}
	err = m.Set(gtid)
	return m, errors.Trace(err)
}

/************************ mysql gtid set ***************************/

// MySQLGTIDSet wraps mysql.MysqlGTIDSet to implement gtidSet interface
// extend some functions to retrieve and compute an intersection with other MySQL GTID Set
type mySQLGTIDSet struct {
	set *mysql.MysqlGTIDSet
}

// replace g by other
func (g *mySQLGTIDSet) Set(other mysql.GTIDSet) error {
	if other == nil {
		return nil
	}

	gs, ok := other.(*mysql.MysqlGTIDSet)
	if !ok {
		return errors.Errorf("%s is not mysql GTID set", other)
	}

	g.set = gs
	return nil
}

func (g *mySQLGTIDSet) Replace(other Set, masters []interface{}) error {
	if other == nil {
		return nil
	}

	otherGS, ok := other.(*mySQLGTIDSet)
	if !ok {
		return errors.Errorf("%s is not mysql GTID set", other)
	}

	for _, uuid := range masters {
		uuidStr, ok := uuid.(string)
		if !ok {
			return errors.Errorf("%v is not string", uuid)
		}

		otherGS.delete(uuidStr)
		if uuidSet, ok := g.get(uuidStr); ok {
			otherGS.set.AddSet(uuidSet)
		}
	}

	for uuid, set := range g.set.Sets {
		if _, ok := otherGS.get(uuid); ok {
			otherGS.delete(uuid)
			otherGS.set.AddSet(set)
		}
	}

	g.set = otherGS.set.Clone().(*mysql.MysqlGTIDSet)
	return nil
}

func (g *mySQLGTIDSet) delete(uuid string) {
	delete(g.set.Sets, uuid)
}

func (g *mySQLGTIDSet) get(uuid string) (*mysql.UUIDSet, bool) {
	uuidSet, ok := g.set.Sets[uuid]
	return uuidSet, ok
}

func (g *mySQLGTIDSet) Clone() Set {
	return &mySQLGTIDSet{
		set: g.set.Clone().(*mysql.MysqlGTIDSet),
	}
}

func (g *mySQLGTIDSet) Origin() mysql.GTIDSet {
	return g.set.Clone().(*mysql.MysqlGTIDSet)
}

func (g *mySQLGTIDSet) String() string {
	return g.set.String()
}

/************************ mariadb gtid set ***************************/
type mariadbGTIDSet struct {
	set *mysql.MariadbGTIDSet
}

// replace g by other
func (m *mariadbGTIDSet) Set(other mysql.GTIDSet) error {
	if other == nil {
		return nil
	}

	gs, ok := other.(*mysql.MariadbGTIDSet)
	if !ok {
		return errors.Errorf("%s is not mariadb GTID set", other)
	}

	m.set = gs
	return nil
}

func (m *mariadbGTIDSet) Replace(other Set, masters []interface{}) error {
	if other == nil {
		return nil
	}

	otherGS, ok := other.(*mariadbGTIDSet)
	if !ok {
		return errors.Errorf("%s is not mariadb GTID set", other)
	}

	for _, id := range masters {
		domainID, ok := id.(uint32)
		if !ok {
			return errors.Errorf("%v is not uint32", id)
		}

		otherGS.delete(domainID)
		if uuidSet, ok := m.get(domainID); ok {
			otherGS.set.AddSet(uuidSet)
		}
	}

	for id, set := range m.set.Sets {
		if _, ok := otherGS.get(id); ok {
			otherGS.delete(id)
			otherGS.set.AddSet(set)
		}
	}

	m.set = otherGS.set.Clone().(*mysql.MariadbGTIDSet)
	return nil
}

func (m *mariadbGTIDSet) delete(domainID uint32) {
	delete(m.set.Sets, domainID)
}

func (m *mariadbGTIDSet) get(domainID uint32) (*mysql.MariadbGTID, bool) {
	gtid, ok := m.set.Sets[domainID]
	return gtid, ok
}

func (m *mariadbGTIDSet) Clone() Set {
	return &mariadbGTIDSet{
		set: m.set.Clone().(*mysql.MariadbGTIDSet),
	}
}

func (m *mariadbGTIDSet) Origin() mysql.GTIDSet {
	return m.set.Clone().(*mysql.MariadbGTIDSet)
}

func (m *mariadbGTIDSet) String() string {
	return m.set.String()
}
