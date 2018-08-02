package filter

import (
	"fmt"
	"regexp"
)

// Table represents a table.
type Table struct {
	Schema string `toml:"db-name" json:"db-name"`
	Name   string `toml:"tbl-name" json:"tbl-name"`
}

// String implements the fmt.Stringer interface.
func (t *Table) String() string {
	return fmt.Sprintf("%s.%s", t.Schema, t.Name)
}

// Rules contains Filter rules.
type Rules struct {
	DoTables []*Table `json:"do-tables"`
	DoDBs    []string `json:"do-dbs"`

	IgnoreTables []*Table `json:"ignore-tables"`
	IgnoreDBs    []string `json:"ignore-dbs"`
}

// Filter implements whitelist and blacklist filters.
type Filter struct {
	patternMap map[string]*regexp.Regexp
	rules      *Rules
}

// New creates a filter use the rules.
func New(rules *Rules) *Filter {
	f := &Filter{}
	f.rules = rules
	f.patternMap = make(map[string]*regexp.Regexp)
	f.genRegexMap()
	return f
}

func (f *Filter) genRegexMap() {
	for _, db := range f.rules.DoDBs {
		f.addOneRegex(db)
	}

	for _, table := range f.rules.DoTables {
		f.addOneRegex(table.Schema)
		f.addOneRegex(table.Name)
	}

	for _, db := range f.rules.IgnoreDBs {
		f.addOneRegex(db)
	}

	for _, table := range f.rules.IgnoreTables {
		f.addOneRegex(table.Schema)
		f.addOneRegex(table.Name)
	}
}

func (f *Filter) addOneRegex(originStr string) {
	if _, ok := f.patternMap[originStr]; !ok {
		var re *regexp.Regexp
		if originStr[0] != '~' {
			re = regexp.MustCompile(fmt.Sprintf("(?i)^%s$", originStr))
		} else {
			re = regexp.MustCompile(fmt.Sprintf("(?i)%s", originStr[1:]))
		}
		f.patternMap[originStr] = re
	}
}

// WhiteFilter whitelist filtering
func (f *Filter) WhiteFilter(stbs []*Table) []*Table {
	var tbs []*Table
	if len(f.rules.DoTables) == 0 && len(f.rules.DoDBs) == 0 {
		return stbs
	}
	for _, tb := range stbs {
		if f.matchTable(f.rules.DoTables, tb) {
			tbs = append(tbs, tb)
		}
		if f.matchDB(f.rules.DoDBs, tb.Schema) {
			tbs = append(tbs, tb)
		}
	}
	return tbs
}

// BlackFilter blacklist filtering
func (f *Filter) BlackFilter(stbs []*Table) []*Table {
	var tbs []*Table
	if len(f.rules.IgnoreDBs) == 0 && len(f.rules.IgnoreTables) == 0 {
		return stbs
	}
	for _, tb := range stbs {
		if f.matchTable(f.rules.IgnoreTables, tb) {
			continue
		}
		if f.matchDB(f.rules.IgnoreDBs, tb.Schema) {
			continue
		}
		tbs = append(tbs, tb)
	}
	return tbs
}

func (f *Filter) matchDB(patternDBS []string, a string) bool {
	for _, b := range patternDBS {
		if f.matchString(b, a) {
			return true
		}
	}
	return false
}

func (f *Filter) matchTable(patternTBS []*Table, tb *Table) bool {
	for _, ptb := range patternTBS {
		if f.matchString(ptb.Schema, tb.Schema) {
			// tb.Name == "" means create or drop database
			if tb.Name == "" || f.matchString(ptb.Name, tb.Name) {
				return true
			}
		}
	}

	return false
}

func (f *Filter) matchString(pattern string, t string) bool {
	if re, ok := f.patternMap[pattern]; ok {
		return re.MatchString(t)
	}
	return pattern == t
}
