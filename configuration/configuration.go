package configuration

import (
	"bytes"
	"fmt"
	"io/ioutil"

	"github.com/pingcap/tidb-tools/pkg/dbutil"
	"github.com/pingcap/tidb-tools/pkg/table-router"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	dm "github.com/pingcap/tidb-enterprise-tools/dm/config"
	"github.com/pingcap/tidb-enterprise-tools/pkg/filter"
	"github.com/pingcap/tidb-enterprise-tools/pkg/utils"
	bf "github.com/pingcap/tidb-tools/pkg/binlog-filter"
	column "github.com/pingcap/tidb-tools/pkg/column-mapping"
	yaml "gopkg.in/yaml.v2"
)

// Table represents a table in a schema of a instance
// if T is empty, it represents a schema
type Table struct {
	InstanceID string `json:"instance-id" toml:"instance-id" yaml:"instance-id"`
	S          string `json:"schema" toml:"schema" yaml:"schema"`
	T          string `json:"table" toml:"table" yaml:"table"`
}

func (table *Table) String() string {
	if len(table.T) == 0 {
		return fmt.Sprintf("%s-`%s`", table.InstanceID, table.S)
	}

	return fmt.Sprintf("%s-`%s`.`%s`", table.InstanceID, table.S, table.T)
}

// TableConfig is simple table configuration contains
// * source table in source database
// * target table in downstream database
// * binlog events that need be ignored
type TableConfig struct {
	Name string `json:"name" toml:"name" yaml:"name"`
	// ignore binlog events dml/ddl
	IgnoredEvents []bf.EventType `json:"ignored-binlog-events"  toml:"ignored-binlog-events" yaml:"ignored-binlog-events"`
}

// InstanceConfig contains all related tables in one instance, it contains:
// * tables
type InstanceConfig struct {
	InstanceID string                    `json:"instance-id" toml:"instance-id" yaml:"instance-id"`
	Tables     map[string][]*TableConfig `json:"tables" toml:"tables" yaml:"tables"`
}

// InstanceShardingConfig contains all related tables in one instance, it contains:
// * column mapping rules
// * tables
type InstanceShardingConfig struct {
	InstanceID         string              `json:"instance-id" toml:"instance-id" yaml:"instance-id"`
	ColumnMappingRules []*column.Rule      `json:"column-mapping-rules" toml:"column-mapping-rules" yaml:"column-mapping-rules"`
	Tables             map[string][]string `json:"tables" toml:"tables" yaml:"tables"`
}

// InstanceTables constains all related tables in one instance, don't have any configurations
type InstanceTables struct {
	InstanceID string              `json:"instance-id" toml:"instance-id" yaml:"instance-id"`
	Tables     map[string][]string `json:"tables" toml:"tables" yaml:"tables"`
}

// ShardingGroup is a sharding group contains:
// * target table {Schema, Table} in downstream database
// * source tables in source instances
type ShardingGroup struct {
	Schema string `json:"schema" toml:"schema" yaml:"schema"`
	Table  string `json:"table" toml:"table" yaml:"table"`

	// instance ID => InstanceConfig
	Sources map[string]*InstanceShardingConfig `json:"source-tables" toml:"source-tables" yaml:"source-tables"`
}

// TablesCount returns tables count
func (s *ShardingGroup) TablesCount() int {
	count := 0
	for _, instance := range s.Sources {
		for _, tables := range instance.Tables {
			count += len(tables)
		}
	}

	return count
}

// DataMigrationConfig is a simple data migration config for human
// so it's tedious in the logical category.
type DataMigrationConfig struct {
	TaskName                 string `yaml:"task-name" toml:"task-name" json:"task-name"`
	TaskMode                 string `yaml:"task-mode" toml:"task-mode" json:"task-mode"`
	Flavor                   string `yaml:"flavor" toml:"flavor" json:"flavor"`
	CheckpointSchemaPrefix   string `yaml:"checkpoint-schema-prefix" toml:"checkpoint-schema-prefix" json:"checkpoint-schema-prefix"`
	RemovePreviousCheckpoint bool   `yaml:"remove-previous-checkpoint" toml:"remove-previous-checkpoint" json:"remove-previous-checkpoint"`
	DisableHeartbeat         bool   `yaml:"disable-heartbeat" toml:"disable-heartbeat" json:"disable-heartbeat"`

	TargetDB *dm.DBConfig `yaml:"target-database" toml:"target-database" json:"target-database"`

	// instanceID -> mysql instance
	MySQLInstances map[string]*dm.MySQLInstance `yaml:"mysql-instances"`

	Mydumpers map[string]*dm.MydumperConfig `yaml:"mydumpers" toml:"mydumpers" json:"mydumpers"`
	Loaders   map[string]*dm.LoaderConfig   `yaml:"loaders" toml:"loaders" json:"loaders"`
	Syncers   map[string]*dm.SyncerConfig   `yaml:"syncers" toml:"syncers" json:"syncers"`

	IgnoredTables map[string]*InstanceTables `json:"ignored-tables" toml:"ignored-tables" yaml:"ignored-tables"`
	DoTables      map[string]*InstanceTables `json:"do-tables" toml:"do-tables" yaml:"do-tables"`

	ShardingGroups []*ShardingGroup `json:"sharding-groups" toml:"sharding-groups" yaml:"sharding-groups"`

	BinlogEventsFilter map[string]*InstanceConfig `json:"filter" toml:"filter" yaml:"filter"`
}

// NewDataMigrationConfig creates a data migration config
func NewDataMigrationConfig() *DataMigrationConfig {
	return &DataMigrationConfig{}
}

// String returns the config's yaml string
func (c *DataMigrationConfig) String() string {
	cfg, err := yaml.Marshal(c)
	if err != nil {
		log.Errorf("marshal data migration config to yaml error %v", err)
	}
	return string(cfg)
}

// DecodeFile loads and decodes config from file
func (c *DataMigrationConfig) DecodeFile(fpath string) error {
	bs, err := ioutil.ReadFile(fpath)
	if err != nil {
		return errors.Annotatef(err, "decode config file %v", fpath)
	}

	err = yaml.Unmarshal(bs, c)
	return errors.Trace(err)
}

// Decode loads config from file data
func (c *DataMigrationConfig) Decode(data string) error {
	err := yaml.Unmarshal([]byte(data), c)
	return errors.Annotatef(err, "decode data %s", data)
}

// GenerateDMTask generates dm task
func (c *DataMigrationConfig) GenerateDMTask() (*dm.TaskConfig, error) {
	if c == nil {
		return nil, nil
	}

	task := &dm.TaskConfig{
		Name:                     c.TaskName,
		TaskMode:                 c.TaskMode,
		Flavor:                   c.Flavor,
		CheckpointSchemaPrefix:   c.CheckpointSchemaPrefix,
		RemovePreviousCheckpoint: c.RemovePreviousCheckpoint,
		DisableHeartbeat:         c.DisableHeartbeat,
		TargetDB:                 new(dm.DBConfig),
		MySQLInstances:           make([]*dm.MySQLInstance, 0, len(c.MySQLInstances)),

		BWList:         make(map[string]*filter.Rules),
		Routes:         make(map[string]*router.TableRule),
		ColumnMappings: make(map[string]*column.Rule),
		Filters:        make(map[string]*bf.BinlogEventRule),

		Mydumpers: make(map[string]*dm.MydumperConfig),
		Loaders:   make(map[string]*dm.LoaderConfig),
		Syncers:   make(map[string]*dm.SyncerConfig),
	}

	*task.TargetDB = *c.TargetDB

	// deepcopy later
	for name, mydumper := range c.Mydumpers {
		task.Mydumpers[name] = new(dm.MydumperConfig)
		*task.Mydumpers[name] = *mydumper
	}

	for name, loader := range c.Loaders {
		task.Loaders[name] = new(dm.LoaderConfig)
		*task.Loaders[name] = *loader
	}

	for name, syncer := range c.Syncers {
		task.Syncers[name] = new(dm.SyncerConfig)
		*task.Syncers[name] = *syncer
	}

	for instanceID, mysqlInstance := range c.MySQLInstances {
		mysqlInstance.ColumnMappingRules = nil
		mysqlInstance.RouteRules = nil
		mysqlInstance.FilterRules = nil
		mysqlInstance.BWListName = ""
		mysqlInstance.InstanceID = instanceID
	}

	err := c.handleBWList(task, c.IgnoredTables, func(bw *filter.Rules, t *Table) {
		if len(t.T) == 0 {
			bw.IgnoreDBs = append(bw.IgnoreDBs, t.S)
		} else {
			bw.IgnoreTables = append(bw.IgnoreTables, &filter.Table{
				Schema: t.S,
				Name:   t.T,
			})
		}
	})
	if err != nil {
		return nil, errors.Trace(err)
	}

	err = c.handleBWList(task, c.DoTables, func(bw *filter.Rules, t *Table) {
		if len(t.T) == 0 {
			bw.DoDBs = append(bw.DoDBs, t.S)
		} else {
			bw.DoTables = append(bw.DoTables, &filter.Table{
				Schema: t.S,
				Name:   t.T,
			})
		}
	})
	if err != nil {
		return nil, errors.Trace(err)
	}

	sharding, err := c.handleShardingGroup(task)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if len(sharding) > 0 {
		task.IsSharding = true
	}

	err = c.handleFilterConfig(task)
	if err != nil {
		return nil, errors.Trace(err)
	}

	/* debug
	log.Infof("########################################################################")
	rawInstance, err := json.MarshalIndent(c.MySQLInstances, "\t", "\t")
	log.Infof("instances %s", rawInstance)

	rawTask, err := json.MarshalIndent(task, "\t", "\t")
	log.Infof("task %s", rawTask)

	rawSharding, err := json.MarshalIndent(sharding, "\t", "\t")
	log.Infof("sharding %s", rawSharding)

	rawRouting, err := json.MarshalIndent(routing, "\t", "\t")
	log.Infof("routing %s", rawRouting)
	log.Infof("########################################################################")
	*/

	err = c.checkShardingGroup(task, sharding)
	if err != nil {
		return nil, errors.Trace(err)
	}

	for instanceID, mysqlInstance := range c.MySQLInstances {
		mysqlInstanceClone := new(dm.MySQLInstance)
		*mysqlInstanceClone = *mysqlInstance
		mysqlInstanceClone.InstanceID = instanceID
		task.MySQLInstances = append(task.MySQLInstances, mysqlInstanceClone)
	}

	return task, nil
}

// DecodeFromTask transforms task to data migrationConfig
// TODO:
// * restore column mapping
// * restore ignored tables
// * restore binlog filter rules
func (c *DataMigrationConfig) DecodeFromTask(task *dm.TaskConfig) error {
	if task == nil {
		return nil
	}

	c.TaskName = task.Name
	c.TaskMode = task.TaskMode
	c.Flavor = task.Flavor
	c.TargetDB = new(dm.DBConfig)
	c.MySQLInstances = make(map[string]*dm.MySQLInstance, len(task.MySQLInstances))
	c.Mydumpers = make(map[string]*dm.MydumperConfig)
	c.Loaders = make(map[string]*dm.LoaderConfig)
	c.Syncers = make(map[string]*dm.SyncerConfig)
	c.DoTables = make(map[string]*InstanceTables)
	c.IgnoredTables = make(map[string]*InstanceTables)
	c.ShardingGroups = nil
	c.BinlogEventsFilter = make(map[string]*InstanceConfig)
	c.CheckpointSchemaPrefix = task.CheckpointSchemaPrefix
	c.RemovePreviousCheckpoint = task.RemovePreviousCheckpoint
	c.DisableHeartbeat = task.DisableHeartbeat

	*c.TargetDB = *task.TargetDB

	for name, mydumper := range task.Mydumpers {
		c.Mydumpers[name] = new(dm.MydumperConfig)
		*c.Mydumpers[name] = *mydumper
	}

	for name, loader := range task.Loaders {
		c.Loaders[name] = new(dm.LoaderConfig)
		*c.Loaders[name] = *loader
	}

	for name, syncer := range task.Syncers {
		c.Syncers[name] = new(dm.SyncerConfig)
		*c.Syncers[name] = *syncer
	}

	mysqlInstances := make(map[string]*dm.MySQLInstance)
	for _, instance := range task.MySQLInstances {
		id := instance.InstanceID
		mysqlInstances[id] = instance
	}
	c.MySQLInstances = mysqlInstances

	sharding, err := c.fetchShardingGroup(mysqlInstances, task)
	if err != nil {
		return errors.Trace(err)
	}

	for target, sg := range sharding {
		targetSchema, targetTable, err := utils.ExtractTable(target)
		if err != nil {
			return errors.Trace(err)
		}

		var shardingGroup *ShardingGroup
		if sg.TablesCount() > 1 {
			shardingGroup = &ShardingGroup{
				Schema:  targetSchema,
				Table:   targetTable,
				Sources: make(map[string]*InstanceShardingConfig),
			}
		}

		for id, instance := range sg.Sources {
			for schema, tables := range instance.Tables {
				for _, table := range tables {
					rules, err := c.fetchFilterRules(mysqlInstances[id], schema, table, task)
					if err != nil {
						return errors.Trace(err)
					}

					// construct sharding group
					if sg.TablesCount() > 1 {
						_, ok := shardingGroup.Sources[id]
						if !ok {
							shardingGroup.Sources[id] = &InstanceShardingConfig{
								Tables: make(map[string][]string),
							}
						}

						shardingGroup.Sources[id].Tables[schema] = append(shardingGroup.Sources[id].Tables[schema], table)
						shardingGroup.Sources[id].InstanceID = id

						cmRules, err := c.fetchColumnMapping(mysqlInstances[id], schema, table, task)
						if err != nil {
							return errors.Trace(err)
						}

						for _, r := range shardingGroup.Sources[id].ColumnMappingRules {
							if _, ok := cmRules[r.PatternSchema]; !ok {
								continue
							}

							if _, ok := cmRules[r.PatternSchema][r.PatternTable]; !ok {
								continue
							}

							delete(cmRules[r.PatternSchema], r.PatternTable)
						}
						for _, rs := range cmRules {
							for _, r := range rs {
								shardingGroup.Sources[id].ColumnMappingRules = append(shardingGroup.Sources[id].ColumnMappingRules, r)
							}
						}
					}

					if len(rules) > 0 {
						st := &TableConfig{
							Name: table,
						}

						for _, rs := range rules {
							for _, r := range rs {
								st.IgnoredEvents = append(st.IgnoredEvents, r.Events...)
							}
						}

						if _, ok := c.BinlogEventsFilter[id]; !ok {
							c.BinlogEventsFilter[id] = &InstanceConfig{
								InstanceID: id,
								Tables:     make(map[string][]*TableConfig),
							}
						}

						c.BinlogEventsFilter[id].Tables[schema] = append(c.BinlogEventsFilter[id].Tables[schema], st)
					}

					if _, ok := c.DoTables[id]; !ok {
						c.DoTables[id] = &InstanceTables{
							InstanceID: id,
							Tables:     make(map[string][]string),
						}
					}

					c.DoTables[id].Tables[schema] = append(c.DoTables[id].Tables[schema], table)
				}
			}
		}
		if sg.TablesCount() > 1 {
			c.ShardingGroups = append(c.ShardingGroups, shardingGroup)
		}
	}

	return nil
}

func (c *DataMigrationConfig) handleBWList(task *dm.TaskConfig, tables map[string]*InstanceTables, fn func(bw *filter.Rules, t *Table)) error {

	for instanceID, instanceTables := range tables {
		if len(instanceID) == 0 {
			return errors.NotValidf("empty instance ID in do/ignored tables")
		}

		instance, ok := c.MySQLInstances[instanceID]
		if !ok {
			return errors.NotFoundf("mysql instance %s", instanceID)
		}
		instance.BWListName = instanceID

		bw := &filter.Rules{}
		if _, ok := task.BWList[instanceID]; ok {
			bw = task.BWList[instanceID]
		} else {
			task.BWList[instanceID] = bw
		}

		for schema, tables := range instanceTables.Tables {
			fn(bw, &Table{
				InstanceID: instanceID,
				S:          schema,
			})

			for _, table := range tables {
				fn(bw, &Table{
					InstanceID: instanceID,
					S:          schema,
					T:          table,
				})
			}
		}
	}

	return nil
}

// handleShardingGroup creates
// * route rule
// * column mapping rule
// for every table in its sharding group
// rule's name is "instance id"-`schema`.`table`
func (c *DataMigrationConfig) handleShardingGroup(task *dm.TaskConfig) (map[string]*ShardingGroup, error) {
	// target name => struct{}
	sharding := make(map[string]*ShardingGroup)
	for _, sg := range c.ShardingGroups {
		targetName := dbutil.TableName(sg.Schema, sg.Table)
		_, ok := sharding[targetName]
		if ok {
			return nil, errors.NotValidf("duplicated sharding group %s", targetName)
		}
		sharding[targetName] = sg

		for instanceID, source := range sg.Sources {
			if len(instanceID) == 0 {
				return nil, errors.NotValidf("empty instance ID in sharding group")
			}
			if _, ok := c.MySQLInstances[instanceID]; !ok {
				return nil, errors.NotFoundf("mysql instance %s in sharding group %s", instanceID, targetName)
			}

			if len(source.ColumnMappingRules) > 1 {
				return nil, errors.NotValidf("sharding group should have only one column mapping, but we got %d", len(source.ColumnMappingRules))
			}

			var columnMapping *column.Rule
			if len(source.ColumnMappingRules) == 1 {
				columnMapping = source.ColumnMappingRules[0]
			}

			for schema, tables := range source.Tables {
				for _, table := range tables {
					t := &Table{
						InstanceID: instanceID,
						S:          schema,
						T:          table,
					}
					name := t.String()

					err := c.genRouteRule(t, sg.Schema, sg.Table, task)
					if err != nil {
						return nil, errors.Trace(err)
					}

					if columnMapping != nil {
						if _, ok := task.ColumnMappings[name]; ok {
							return nil, errors.AlreadyExistsf("column mapping rule for %s, already exist one is %s, another one is %s", name, task.ColumnMappings[name].TargetColumn, columnMapping.TargetColumn)
						}
						mapping := new(column.Rule)
						task.ColumnMappings[name] = mapping

						*mapping = *columnMapping
						mapping.PatternSchema = t.S
						mapping.PatternTable = t.T
						c.MySQLInstances[instanceID].ColumnMappingRules = append(c.MySQLInstances[instanceID].ColumnMappingRules, name)
					}
				}
			}
		}
	}

	for name, s := range sharding {
		if s.TablesCount() <= 1 {
			delete(sharding, name)
		}
	}

	return sharding, nil
}

// handleFilterConfig creates
// * route rule
// * binlog event filter rule
// for every table
func (c *DataMigrationConfig) handleFilterConfig(task *dm.TaskConfig) error {
	for instanceID, instance := range c.BinlogEventsFilter {
		if len(instanceID) == 0 {
			return errors.NotValidf("empty instance ID in table config")
		}

		if _, ok := c.MySQLInstances[instanceID]; !ok {
			return errors.NotFoundf("mysql instance %s in table configs", instanceID)
		}

		for schema, tables := range instance.Tables {
			for _, table := range tables {
				t := &Table{
					InstanceID: instanceID,
					S:          schema,
					T:          table.Name,
				}

				name := t.String()
				if _, ok := task.Filters[name]; ok {
					return errors.AlreadyExistsf("binlog event filyter rule for %s", name)
				}
				task.Filters[name] = &bf.BinlogEventRule{
					SchemaPattern: t.S,
					TablePattern:  t.T,
					Events:        table.IgnoredEvents,
					Action:        bf.Ignore,
				}
				c.MySQLInstances[instanceID].FilterRules = append(c.MySQLInstances[instanceID].FilterRules, name)
			}
		}
	}

	return nil
}

// expectedRouting: target => [instance, schema, table]
func (c *DataMigrationConfig) checkShardingGroup(task *dm.TaskConfig, expectedSharding map[string]*ShardingGroup) error {
	sharding, err := c.fetchShardingGroup(c.MySQLInstances, task)
	if err != nil {
		return errors.Trace(err)
	}

	// delete non-sharding group
	for target, gs := range sharding {
		if gs.TablesCount() == 1 {
			delete(sharding, target)
		}
	}

	var msg bytes.Buffer
	if len(expectedSharding) != len(sharding) {
		fmt.Fprintf(&msg, "have sharding group %+v, but expected %+v, please check your ignore tables and table config\n", sharding, expectedSharding)
	}

	for target, expectedGroup := range expectedSharding {
		group, ok := sharding[target]
		if !ok {
			fmt.Fprintf(&msg, "[sharding group target %s] sharding group is not found, please check your ignore tables and table config\n", target)
			continue
		}

		if group.TablesCount() != expectedGroup.TablesCount() {
			fmt.Fprintf(&msg, "[sharding group target %s] sharding tables count %d, but expected %d please check your ignore tables and table configs\n", target, group.TablesCount(), expectedGroup.TablesCount())
		}

		if len(group.Sources) != len(expectedGroup.Sources) {
			fmt.Fprintf(&msg, "[sharding group target %s] instance %+v, but expected %+v please check your ignore tables and table configs\n", target, group.Sources, expectedGroup.Sources)
			continue
		}

		for instanceID, expectedInstance := range expectedGroup.Sources {
			instance, ok := group.Sources[instanceID]
			if !ok {
				fmt.Fprintf(&msg, "[sharding group target %s] instance %s is not found, please check your ignore tables\n", target, instanceID)
				continue
			}

			if len(instance.Tables) != len(expectedInstance.Tables) {
				fmt.Fprintf(&msg, "[sharding group target %s]  instance %s has schemas %+v, but expected %+v please check your ignore tables and table configs\n", target, instance, instance.Tables, expectedInstance.Tables)
				continue
			}

			for schema, expectedTables := range expectedInstance.Tables {
				tables, ok := instance.Tables[schema]
				if !ok {
					fmt.Fprintf(&msg, "[sharding group target %s] instance %s 's schema %s is not found, please check your ignore tables and table configs\n", target, instanceID, schema)
					continue
				}

				if len(tables) != len(expectedTables) {
					fmt.Fprintf(&msg, "[sharding group target %s] instance %s 's schema %s has tables %d, but expected %d please check your ignore tables and table configs\n", target, instanceID, schema, len(tables), len(expectedTables))
					continue
				}
			}
		}
	}

	if msg.Len() == 0 {
		return nil
	}

	return errors.NotValidf("sharding check failed\n %s", msg.String())
}

// fetchShardingGroup fetchs tables from source database
func (c *DataMigrationConfig) fetchShardingGroup(instances map[string]*dm.MySQLInstance, task *dm.TaskConfig) (map[string]*ShardingGroup, error) {
	sharding := make(map[string]*ShardingGroup)
	for id, instance := range instances {
		bw := filter.New(task.BWList[instance.BWListName])
		routeRules := make([]*router.TableRule, 0, len(instance.RouteRules))
		for _, name := range instance.RouteRules {
			routeRules = append(routeRules, task.Routes[name])
		}

		r, err := router.NewTableRouter(routeRules)
		if err != nil {
			return nil, errors.Trace(err)
		}

		sourceDBinfo := &dbutil.DBConfig{
			Host:     instance.Config.Host,
			Port:     instance.Config.Port,
			User:     instance.Config.User,
			Password: instance.Config.Password,
		}
		sourceDB, err := dbutil.OpenDB(*sourceDBinfo)
		if err != nil {
			return nil, errors.Trace(err)
		}

		mapping, err := utils.FetchTargetDoTables(sourceDB, bw, r)
		if err != nil {
			sourceDB.Close()
			return nil, errors.Trace(err)
		}

		for name, tables := range mapping {
			for _, table := range tables {
				gs, ok := sharding[name]
				if !ok {
					gs = &ShardingGroup{
						Sources: make(map[string]*InstanceShardingConfig),
					}
					sharding[name] = gs
				}

				if _, ok := gs.Sources[id]; !ok {
					gs.Sources[id] = &InstanceShardingConfig{
						Tables: make(map[string][]string),
					}
				}

				gs.Sources[id].Tables[table.Schema] = append(gs.Sources[id].Tables[table.Schema], table.Name)
			}
		}
		sourceDB.Close()
	}

	return sharding, nil
}

func (c *DataMigrationConfig) fetchColumnMapping(instance *dm.MySQLInstance, schema, table string, task *dm.TaskConfig) (map[string]map[string]*column.Rule, error) {
	rules := make([]*column.Rule, 0, len(instance.ColumnMappingRules))
	for _, name := range instance.ColumnMappingRules {
		rules = append(rules, task.ColumnMappings[name])
	}

	m, err := column.NewMapping(rules)
	if err != nil {
		return nil, errors.Trace(err)
	}

	rs := m.Match(schema, table)
	res := make(map[string]map[string]*column.Rule)
	for _, rule := range rs {
		columnRule, ok := rule.(*column.Rule)
		if !ok {
			return nil, errors.NotValidf("rule %+v", rule)
		}
		if _, ok := res[columnRule.PatternSchema]; !ok {
			res[columnRule.PatternSchema] = make(map[string]*column.Rule)
		}
		res[columnRule.PatternSchema][columnRule.PatternTable] = columnRule
	}

	return res, nil
}

func (c *DataMigrationConfig) fetchFilterRules(instance *dm.MySQLInstance, schema, table string, task *dm.TaskConfig) (map[string]map[string]*bf.BinlogEventRule, error) {
	rules := make([]*bf.BinlogEventRule, 0, len(instance.FilterRules))
	for _, name := range instance.FilterRules {
		rules = append(rules, task.Filters[name])
	}

	b, err := bf.NewBinlogEvent(rules)
	if err != nil {
		return nil, errors.Trace(err)
	}

	rs := b.Match(schema, table)
	res := make(map[string]map[string]*bf.BinlogEventRule)
	for _, rule := range rs {
		binlogEventRule, ok := rule.(*bf.BinlogEventRule)
		if !ok {
			return nil, errors.NotValidf("rule %+v", rule)
		}
		if binlogEventRule.Action == bf.Do {
			continue
		}

		if _, ok := res[binlogEventRule.SchemaPattern]; !ok {
			res[binlogEventRule.SchemaPattern] = make(map[string]*bf.BinlogEventRule)
		}
		res[binlogEventRule.SchemaPattern][binlogEventRule.TablePattern] = binlogEventRule
	}

	return res, nil
}

func (c *DataMigrationConfig) genRouteRule(t *Table, targetSchema string, targetTable string, task *dm.TaskConfig) error {
	name := t.String()
	if _, ok := task.Routes[name]; ok {
		return errors.AlreadyExistsf("route rule for %s, already exist one is %s.%s, another one is %s.%s", name, task.Routes[name].TargetSchema, task.Routes[name].TargetTable, targetSchema, targetTable)
	}
	schemaRoute := &Table{
		InstanceID: t.InstanceID,
		S:          t.S,
	}
	schemaName := fmt.Sprintf("%s-`%s`.`%s`", schemaRoute.String(), targetSchema, targetTable)

	task.Routes[name] = &router.TableRule{
		SchemaPattern: t.S,
		TablePattern:  t.T,
		TargetSchema:  targetSchema,
		TargetTable:   targetTable,
	}

	c.MySQLInstances[t.InstanceID].RouteRules = append(c.MySQLInstances[t.InstanceID].RouteRules, name)

	if _, ok := task.Routes[schemaName]; !ok {
		task.Routes[schemaName] = &router.TableRule{
			SchemaPattern: t.S,
			TargetSchema:  targetSchema,
		}
		c.MySQLInstances[t.InstanceID].RouteRules = append(c.MySQLInstances[t.InstanceID].RouteRules, schemaName)
	}

	return nil
}
