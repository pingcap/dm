package portal

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"sort"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	. "github.com/pingcap/check"
	bf "github.com/pingcap/tidb-tools/pkg/binlog-filter"
	"github.com/pingcap/tidb-tools/pkg/filter"
	router "github.com/pingcap/tidb-tools/pkg/table-router"
)

var _ = Suite(&testPortalSuite{})

type testPortalSuite struct {
	portalHandler *Handler

	allTables []TablesInSchema

	taskConfig *DMTaskConfig
}

func TestSuite(t *testing.T) {
	TestingT(t)
}

func (t *testPortalSuite) SetUpSuite(c *C) {
	t.portalHandler = NewHandler(c.MkDir(), 10)
	t.allTables = []TablesInSchema{
		{
			Schema: "db_1",
			Tables: []string{"t_1", "t_2"},
		}, {
			Schema: "db_2",
			Tables: []string{},
		}, {
			Schema: "db_3",
			Tables: []string{"t_3"},
		}, {
			Schema: "mysql",
			Tables: []string{"user"},
		},
	}

	t.initTaskCfg()
}

func (t *testPortalSuite) initTaskCfg() {
	t.taskConfig = &DMTaskConfig{
		Name:     "unit-test",
		TaskMode: "all",
		TargetDB: &DBConfig{
			Host:     "127.0.0.2",
			Port:     4000,
			User:     "root",
			Password: "123456",
		},
		MySQLInstances: []*MySQLInstance{
			{
				SourceID: "source-1",
				Meta: &Meta{
					BinLogName: "log-bin.00001",
					BinLogPos:  123,
				},
			}, {
				SourceID: "source-2",
				Meta: &Meta{
					BinLogName: "log-bin.00002",
					BinLogPos:  456,
				},
			},
		},
		Routes: map[string]*router.TableRule{
			"source-1.route_rules.1": {
				TargetSchema: "db_1",
				TargetTable:  "t_1",
			},
			"source-1.route_rules.2": {},
			"source-2.route_rules.1": {
				TargetSchema: "db_1",
				TargetTable:  "t_1",
			},
		},
		Filters: map[string]*bf.BinlogEventRule{
			"source-1.filter.1": {},
			"source-2.filter.1": {},
			"source-2.filter.2": {},
		},
		BWList: map[string]*filter.Rules{
			"source-1.bw_list.1": {
				DoTables: []*filter.Table{
					{
						Schema: "db_1",
						Name:   "t_1",
					}, {
						Schema: "db_1",
						Name:   "t_2",
					},
				},
			},
			"source-2.bw_list.1": {
				DoTables: []*filter.Table{
					{
						Schema: "db_1",
						Name:   "t_1",
					}, {
						Schema: "db_1",
						Name:   "t_3",
					},
				},
			},
		},
	}
}

func (t *testPortalSuite) TestCheck(c *C) {
	dbCfgBytes := getTestDBCfgBytes(c)
	req := httptest.NewRequest("POST", "/check", bytes.NewReader(dbCfgBytes))
	resp := httptest.NewRecorder()

	t.portalHandler.Check(resp, req)
	c.Assert(resp.Code, Equals, http.StatusBadRequest)

	checkResult := &CheckResult{}
	err := readJSON(resp.Body, checkResult)
	c.Assert(err, IsNil)
	c.Assert(checkResult.Result, Equals, failed)
	c.Assert(checkResult.Error, Equals, "Error 1045: Access denied for user 'root'@'127.0.0.1' (using password: YES)")

	getDBConnFunc = t.getMockDB
	defer func() {
		getDBConnFunc = getDBConnFromReq
	}()

	resp = httptest.NewRecorder()
	t.portalHandler.Check(resp, req)
	c.Assert(resp.Code, Equals, http.StatusOK)

	err = readJSON(resp.Body, checkResult)
	c.Assert(err, IsNil)
	c.Assert(checkResult.Result, Equals, success)
	c.Assert(checkResult.Error, Equals, "")
}

func (t *testPortalSuite) TestGetSchemaInfo(c *C) {
	dbCfgBytes := getTestDBCfgBytes(c)
	req := httptest.NewRequest("POST", "/schema", bytes.NewReader(dbCfgBytes))
	resp := httptest.NewRecorder()

	t.portalHandler.GetSchemaInfo(resp, req)
	c.Assert(resp.Code, Equals, http.StatusBadRequest)

	schemaInfoResult := new(SchemaInfoResult)
	err := readJSON(resp.Body, schemaInfoResult)
	c.Assert(err, IsNil)
	c.Assert(schemaInfoResult.Result, Equals, failed)
	c.Assert(schemaInfoResult.Error, Equals, "Error 1045: Access denied for user 'root'@'127.0.0.1' (using password: YES)")
	c.Assert(schemaInfoResult.Tables, IsNil)

	getDBConnFunc = t.getMockDB
	defer func() {
		getDBConnFunc = getDBConnFromReq
	}()

	resp = httptest.NewRecorder()
	t.portalHandler.GetSchemaInfo(resp, req)
	c.Assert(resp.Code, Equals, http.StatusOK)

	err = readJSON(resp.Body, schemaInfoResult)
	c.Assert(err, IsNil)
	c.Assert(schemaInfoResult.Result, Equals, success)
	c.Assert(schemaInfoResult.Error, Equals, "")
	c.Assert(schemaInfoResult.Tables, HasLen, len(t.allTables)-1)
	for i, schemaTables := range schemaInfoResult.Tables {
		c.Assert(schemaTables.Schema, Equals, t.allTables[i].Schema)
		for j, table := range schemaTables.Tables {
			c.Assert(table, Equals, t.allTables[i].Tables[j])
		}
	}
}

func (t *testPortalSuite) TestGenerateAndDownloadAndAnalyzeConfig(c *C) {
	t.initTaskCfg()

	// test generate config file
	cfgBytes, err := json.Marshal(t.taskConfig)
	c.Assert(err, IsNil)
	req := httptest.NewRequest("POST", "/generate_config", bytes.NewReader(cfgBytes))
	resp := httptest.NewRecorder()
	t.portalHandler.GenerateConfig(resp, req)
	c.Assert(resp.Code, Equals, http.StatusOK)

	generateConfigResult := new(GenerateConfigResult)
	err = readJSON(resp.Body, generateConfigResult)
	c.Assert(err, IsNil)
	c.Assert(generateConfigResult.Result, Equals, success)

	// test download
	req = httptest.NewRequest("POST", "/download?filepath="+generateConfigResult.Filepath, nil)
	resp = httptest.NewRecorder()
	t.portalHandler.Download(resp, req)
	c.Assert(resp.Code, Equals, http.StatusOK)
	c.Log(resp.Body)

	// generate upload config file request and test analyze config
	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	part, err := writer.CreateFormFile("taskfile", "task.yaml")
	c.Assert(err, IsNil)
	io.Copy(part, resp.Body)
	writer.Close()
	req = httptest.NewRequest("POST", "/analyze_config_file", body)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	resp = httptest.NewRecorder()
	t.portalHandler.AnalyzeConfig(resp, req)
	c.Assert(resp.Code, Equals, http.StatusOK)

	analyzeResult := new(AnalyzeResult)
	err = readJSON(resp.Body, analyzeResult)
	c.Assert(err, IsNil)
	c.Log(analyzeResult.Config)

	c.Assert(analyzeResult.Config.Name, Equals, t.taskConfig.Name)
	c.Assert(analyzeResult.Config.TaskMode, Equals, t.taskConfig.TaskMode)
	c.Assert(*analyzeResult.Config.TargetDB, DeepEquals, *t.taskConfig.TargetDB)
	c.Assert(*analyzeResult.Config.MySQLInstances[0].Meta, DeepEquals, *t.taskConfig.MySQLInstances[0].Meta)
	c.Assert(*analyzeResult.Config.MySQLInstances[1].Meta, DeepEquals, *t.taskConfig.MySQLInstances[1].Meta)

	sort.Strings(analyzeResult.Config.MySQLInstances[0].FilterRules)
	sort.Strings(analyzeResult.Config.MySQLInstances[0].RouteRules)
	sort.Strings(analyzeResult.Config.MySQLInstances[1].FilterRules)
	sort.Strings(analyzeResult.Config.MySQLInstances[1].RouteRules)
	c.Assert(analyzeResult.Config.MySQLInstances[0].FilterRules, DeepEquals, []string{"source-1.filter.1"})
	c.Assert(analyzeResult.Config.MySQLInstances[0].RouteRules, DeepEquals, []string{"source-1.route_rules.1", "source-1.route_rules.2"})
	c.Assert(analyzeResult.Config.MySQLInstances[0].BWListName, DeepEquals, "source-1.bw_list.1")
	c.Assert(analyzeResult.Config.MySQLInstances[1].FilterRules, DeepEquals, []string{"source-2.filter.1", "source-2.filter.2"})
	c.Assert(analyzeResult.Config.MySQLInstances[1].RouteRules, DeepEquals, []string{"source-2.route_rules.1"})
	c.Assert(analyzeResult.Config.MySQLInstances[1].BWListName, DeepEquals, "source-2.bw_list.1")
}

func (t *testPortalSuite) TestAnalyzeRuleName(c *C) {
	testCases := []struct {
		name     string
		sourceID string
		tp       string
		valid    bool
	}{
		{
			name:     "source-1.route_rules.1",
			sourceID: "source-1",
			tp:       "route_rules",
			valid:    true,
		}, {
			name:  "source-1.route_rule.1",
			valid: false,
		}, {
			name:  "source.1.route_rule.1",
			valid: false,
		},
	}

	for _, testCase := range testCases {
		sourceID, tp, err := analyzeRuleName(testCase.name)
		c.Assert(err == nil, Equals, testCase.valid)
		if err != nil {
			c.Assert(testCase.sourceID, Equals, sourceID)
			c.Assert(testCase.tp, Equals, tp)
		}
	}
}

func (t *testPortalSuite) getMockDB(req *http.Request, timeout int) (*sql.DB, string, error) {
	db, mock, err := sqlmock.New()
	if err != nil {
		return nil, "", err
	}
	t.mockSchemaInfo(mock)

	return db, "mock", nil
}

func (t *testPortalSuite) TestAdjustConfig(c *C) {
	adjustConfig(t.taskConfig)

	// test mysql instance's filter rules, route rules, bw list and mydumper config name
	c.Assert(t.taskConfig.IsSharding, IsTrue)
	sort.Strings(t.taskConfig.MySQLInstances[0].FilterRules)
	sort.Strings(t.taskConfig.MySQLInstances[0].RouteRules)
	sort.Strings(t.taskConfig.MySQLInstances[1].FilterRules)
	sort.Strings(t.taskConfig.MySQLInstances[1].RouteRules)
	c.Assert(t.taskConfig.MySQLInstances[0].FilterRules, DeepEquals, []string{"source-1.filter.1"})
	c.Assert(t.taskConfig.MySQLInstances[0].RouteRules, DeepEquals, []string{"source-1.route_rules.1", "source-1.route_rules.2"})
	c.Assert(t.taskConfig.MySQLInstances[0].BWListName, Equals, "source-1.bw_list.1")
	c.Assert(t.taskConfig.MySQLInstances[0].MydumperConfigName, Equals, "source-1.dump")
	c.Assert(t.taskConfig.MySQLInstances[1].FilterRules, DeepEquals, []string{"source-2.filter.1", "source-2.filter.2"})
	c.Assert(t.taskConfig.MySQLInstances[1].RouteRules, DeepEquals, []string{"source-2.route_rules.1"})
	c.Assert(t.taskConfig.MySQLInstances[1].BWListName, Equals, "source-2.bw_list.1")
	c.Assert(t.taskConfig.MySQLInstances[1].MydumperConfigName, Equals, "source-2.dump")

	// test generated mydumper config
	c.Assert(t.taskConfig.Mydumpers, HasLen, 2)
	dumpCfg, ok := t.taskConfig.Mydumpers["source-1.dump"]
	c.Assert(ok, IsTrue)
	c.Assert(dumpCfg.ExtraArgs, Equals, "-T db_1.t_1,db_1.t_2")
	dumpCfg, ok = t.taskConfig.Mydumpers["source-2.dump"]
	c.Assert(ok, IsTrue)
	c.Assert(dumpCfg.ExtraArgs, Equals, "-T db_1.t_1,db_1.t_3")
}

func (t *testPortalSuite) TestGenerateMydumperTableCfg(c *C) {
	bwList := &filter.Rules{
		DoTables: []*filter.Table{
			{
				Schema: "db_1",
				Name:   "t_1",
			}, {
				Schema: "db_1",
				Name:   "t_2",
			},
		},
	}
	mydumperCfg := generateMydumperCfg(bwList)
	c.Assert(mydumperCfg.ExtraArgs, Equals, "-T db_1.t_1,db_1.t_2")

	bwList = &filter.Rules{}
	mydumperCfg = generateMydumperCfg(bwList)
	c.Assert(mydumperCfg.ExtraArgs, Equals, "")
}

func (t *testPortalSuite) TestGenerateMydumperCfgName(c *C) {
	dumpCfgName := generateMydumperCfgName("source-1")
	c.Assert(dumpCfgName, Equals, "source-1.dump")
}

func (t *testPortalSuite) mockSchemaInfo(mock sqlmock.Sqlmock) {
	schemas := sqlmock.NewRows([]string{"Database"})
	for _, tables := range t.allTables {
		schemas.AddRow(tables.Schema)
	}
	mock.ExpectQuery("SHOW DATABASES").WillReturnRows(schemas)

	for _, tables := range t.allTables {
		tablesResult := sqlmock.NewRows([]string{"Tables_in_" + tables.Schema, "Table_type"})
		for _, table := range tables.Tables {
			tablesResult.AddRow(table, "BASE TABLE")
		}
		mock.ExpectQuery("SHOW FULL TABLES").WillReturnRows(tablesResult)
	}
}

func (t *testPortalSuite) TestGenerateTaskFileName(c *C) {
	taskName := "test"
	fileName := generateTaskFileName(taskName)
	c.Assert(fileName, Equals, "test-task.yaml")
}

func getTestDBCfgBytes(c *C) []byte {
	dbCfg := &DBConfig{
		Host:     "127.0.0.1",
		Port:     3306,
		User:     "root",
		Password: "123456",
	}
	dbCfgBytes, err := json.Marshal(dbCfg)
	c.Assert(err, IsNil)

	return dbCfgBytes
}
