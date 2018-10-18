package filter

import (
	"fmt"
	"regexp"
	"strings"
	"sync"
)

// ActionType is do or ignore something
type ActionType bool

// builtin actiontype variable
const (
	Do     ActionType = true
	Ignore ActionType = false
)

// Table represents a table.
type Table struct {
	Schema string `toml:"db-name" json:"db-name" yaml:"db-name"`
	Name   string `toml:"tbl-name" json:"tbl-name" yaml:"tbl-name"`
}

// String implements the fmt.Stringer interface.
func (t *Table) String() string {
	if len(t.Name) > 0 {
		return fmt.Sprintf("`%s`.`%s`", t.Schema, t.Name)
	}
	return fmt.Sprintf("`%s`", t.Schema)
}

type cache struct {
	sync.RWMutex
	items map[string]ActionType // `schema`.`table` => do/ignore
}

func (c *cache) query(key string) (ActionType, bool) {
	c.RLock()
	action, exist := c.items[key]
	c.RUnlock()

	return action, exist
}

func (c *cache) set(key string, action ActionType) {
	c.Lock()
	c.items[key] = action
	c.Unlock()
}

// Rules contains Filter rules.
type Rules struct {
	DoTables []*Table `json:"do-tables" toml:"do-tables" yaml:"do-tables"`
	DoDBs    []string `json:"do-dbs" toml:"do-dbs" yaml:"do-dbs"`

	IgnoreTables []*Table `json:"ignore-tables" toml:"ignore-tables" yaml:"ignore-tables"`
	IgnoreDBs    []string `json:"ignore-dbs" toml:"ignore-dbs" yaml:"ignore-dbs"`
}

// ToLower convert all entries to lowercase
func (r *Rules) ToLower() {
	if r == nil {
		return
	}

	for _, table := range r.DoTables {
		table.Name = strings.ToLower(table.Name)
		table.Schema = strings.ToLower(table.Schema)
	}
	for _, table := range r.IgnoreTables {
		table.Name = strings.ToLower(table.Name)
		table.Schema = strings.ToLower(table.Schema)
	}
	for i, db := range r.IgnoreDBs {
		r.IgnoreDBs[i] = strings.ToLower(db)
	}
	for i, db := range r.DoDBs {
		r.DoDBs[i] = strings.ToLower(db)
	}
}

// Filter implements whitelist and blacklist filters.
type Filter struct {
	patternMap map[string]*regexp.Regexp
	rules      *Rules

	c *cache

	caseSensitive bool
}

// New creates a filter use the rules.
func New(caseSensitive bool, rules *Rules) *Filter {
	f := &Filter{
		caseSensitive: caseSensitive,
		rules:         rules,
	}
	if !f.caseSensitive {
		f.rules.ToLower()
	}

	f.patternMap = make(map[string]*regexp.Regexp)
	f.c = &cache{
		items: make(map[string]ActionType),
	}
	f.genRegexMap()
	return f
}

func (f *Filter) genRegexMap() {
	if f.rules == nil {
		return
	}

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

// ApplyOn applies filter rules on tables
// rules like
// https://dev.mysql.com/doc/refman/8.0/en/replication-rules-table-options.html
// https://dev.mysql.com/doc/refman/8.0/en/replication-rules-db-options.html
func (f *Filter) ApplyOn(stbs []*Table) []*Table {
	if f == nil || f.rules == nil {
		return stbs
	}

	var tbs []*Table
	for _, tb := range stbs {
		name := tb.String()
		do, exist := f.c.query(name)
		if !exist {
			tbCopy := &Table{Schema: tb.Schema, Name: tb.Name}
			if !f.caseSensitive {
				tbCopy = &Table{Schema: strings.ToLower(tb.Schema), Name: strings.ToLower(tb.Name)}
			}

			do = ActionType(f.filterOnSchemas(tbCopy) && f.filterOnTables(tbCopy))
			f.c.set(tb.String(), do)
		}

		if do {
			tbs = append(tbs, tb)
		}
	}

	return tbs
}

func (f *Filter) filterOnSchemas(tb *Table) bool {
	if len(f.rules.DoDBs) > 0 {
		// not macthed do db rules, ignore update
		if !f.findMatchedDoDBs(tb) {
			return false
		}
	} else if len(f.rules.IgnoreDBs) > 0 {
		//  macthed ignore db rules, ignore update
		if f.findMatchedIgnoreDBs(tb) {
			return false
		}
	}

	return true
}

func (f *Filter) findMatchedDoDBs(tb *Table) bool {
	return f.matchDB(f.rules.DoDBs, tb.Schema)
}

func (f *Filter) findMatchedIgnoreDBs(tb *Table) bool {
	return f.matchDB(f.rules.IgnoreDBs, tb.Schema)
}

func (f *Filter) filterOnTables(tb *Table) bool {
	// schema statement like create/drop/alter database
	if len(tb.Name) == 0 {
		return true
	}

	if len(f.rules.DoTables) > 0 {
		if f.findMatchedDoTables(tb) {
			return true
		}
	}

	if len(f.rules.IgnoreTables) > 0 {
		if f.findMatchedIgnoreTables(tb) {
			return false
		}
	}

	return len(f.rules.DoTables) == 0
}

func (f *Filter) findMatchedDoTables(tb *Table) bool {
	return f.matchTable(f.rules.DoTables, tb)
}

func (f *Filter) findMatchedIgnoreTables(tb *Table) bool {
	return f.matchTable(f.rules.IgnoreTables, tb)
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
		if f.matchString(ptb.Schema, tb.Schema) && f.matchString(ptb.Name, tb.Name) {
			return true
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
