// Code generated by MockGen. DO NOT EDIT.
// Source: github.com/pingcap/dm/dm/pb (interfaces: MasterClient,MasterServer)

// Package pbmock is a generated GoMock package.
package pbmock

import (
	context "context"
	gomock "github.com/golang/mock/gomock"
	pb "github.com/pingcap/dm/dm/pb"
	grpc "google.golang.org/grpc"
	reflect "reflect"
)

// MockMasterClient is a mock of MasterClient interface
type MockMasterClient struct {
	ctrl     *gomock.Controller
	recorder *MockMasterClientMockRecorder
}

// MockMasterClientMockRecorder is the mock recorder for MockMasterClient
type MockMasterClientMockRecorder struct {
	mock *MockMasterClient
}

// NewMockMasterClient creates a new mock instance
func NewMockMasterClient(ctrl *gomock.Controller) *MockMasterClient {
	mock := &MockMasterClient{ctrl: ctrl}
	mock.recorder = &MockMasterClientMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use
func (m *MockMasterClient) EXPECT() *MockMasterClientMockRecorder {
	return m.recorder
}

// CheckTask mocks base method
func (m *MockMasterClient) CheckTask(arg0 context.Context, arg1 *pb.CheckTaskRequest, arg2 ...grpc.CallOption) (*pb.CheckTaskResponse, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{arg0, arg1}
	for _, a := range arg2 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "CheckTask", varargs...)
	ret0, _ := ret[0].(*pb.CheckTaskResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// CheckTask indicates an expected call of CheckTask
func (mr *MockMasterClientMockRecorder) CheckTask(arg0, arg1 interface{}, arg2 ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{arg0, arg1}, arg2...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CheckTask", reflect.TypeOf((*MockMasterClient)(nil).CheckTask), varargs...)
}

// GetSubTaskCfg mocks base method
func (m *MockMasterClient) GetSubTaskCfg(arg0 context.Context, arg1 *pb.GetSubTaskCfgRequest, arg2 ...grpc.CallOption) (*pb.GetSubTaskCfgResponse, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{arg0, arg1}
	for _, a := range arg2 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "GetSubTaskCfg", varargs...)
	ret0, _ := ret[0].(*pb.GetSubTaskCfgResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetSubTaskCfg indicates an expected call of GetSubTaskCfg
func (mr *MockMasterClientMockRecorder) GetSubTaskCfg(arg0, arg1 interface{}, arg2 ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{arg0, arg1}, arg2...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetSubTaskCfg", reflect.TypeOf((*MockMasterClient)(nil).GetSubTaskCfg), varargs...)
}

// GetTaskCfg mocks base method
func (m *MockMasterClient) GetTaskCfg(arg0 context.Context, arg1 *pb.GetTaskCfgRequest, arg2 ...grpc.CallOption) (*pb.GetTaskCfgResponse, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{arg0, arg1}
	for _, a := range arg2 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "GetTaskCfg", varargs...)
	ret0, _ := ret[0].(*pb.GetTaskCfgResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetTaskCfg indicates an expected call of GetTaskCfg
func (mr *MockMasterClientMockRecorder) GetTaskCfg(arg0, arg1 interface{}, arg2 ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{arg0, arg1}, arg2...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetTaskCfg", reflect.TypeOf((*MockMasterClient)(nil).GetTaskCfg), varargs...)
}

// HandleSQLs mocks base method
func (m *MockMasterClient) HandleSQLs(arg0 context.Context, arg1 *pb.HandleSQLsRequest, arg2 ...grpc.CallOption) (*pb.HandleSQLsResponse, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{arg0, arg1}
	for _, a := range arg2 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "HandleSQLs", varargs...)
	ret0, _ := ret[0].(*pb.HandleSQLsResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// HandleSQLs indicates an expected call of HandleSQLs
func (mr *MockMasterClientMockRecorder) HandleSQLs(arg0, arg1 interface{}, arg2 ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{arg0, arg1}, arg2...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "HandleSQLs", reflect.TypeOf((*MockMasterClient)(nil).HandleSQLs), varargs...)
}

// ListMember mocks base method
func (m *MockMasterClient) ListMember(arg0 context.Context, arg1 *pb.ListMemberRequest, arg2 ...grpc.CallOption) (*pb.ListMemberResponse, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{arg0, arg1}
	for _, a := range arg2 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "ListMember", varargs...)
	ret0, _ := ret[0].(*pb.ListMemberResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ListMember indicates an expected call of ListMember
func (mr *MockMasterClientMockRecorder) ListMember(arg0, arg1 interface{}, arg2 ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{arg0, arg1}, arg2...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ListMember", reflect.TypeOf((*MockMasterClient)(nil).ListMember), varargs...)
}

// MigrateWorkerRelay mocks base method
func (m *MockMasterClient) MigrateWorkerRelay(arg0 context.Context, arg1 *pb.MigrateWorkerRelayRequest, arg2 ...grpc.CallOption) (*pb.CommonWorkerResponse, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{arg0, arg1}
	for _, a := range arg2 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "MigrateWorkerRelay", varargs...)
	ret0, _ := ret[0].(*pb.CommonWorkerResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// MigrateWorkerRelay indicates an expected call of MigrateWorkerRelay
func (mr *MockMasterClientMockRecorder) MigrateWorkerRelay(arg0, arg1 interface{}, arg2 ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{arg0, arg1}, arg2...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "MigrateWorkerRelay", reflect.TypeOf((*MockMasterClient)(nil).MigrateWorkerRelay), varargs...)
}

// OfflineMember mocks base method
func (m *MockMasterClient) OfflineMember(arg0 context.Context, arg1 *pb.OfflineMemberRequest, arg2 ...grpc.CallOption) (*pb.OfflineMemberResponse, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{arg0, arg1}
	for _, a := range arg2 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "OfflineMember", varargs...)
	ret0, _ := ret[0].(*pb.OfflineMemberResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// OfflineMember indicates an expected call of OfflineMember
func (mr *MockMasterClientMockRecorder) OfflineMember(arg0, arg1 interface{}, arg2 ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{arg0, arg1}, arg2...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "OfflineMember", reflect.TypeOf((*MockMasterClient)(nil).OfflineMember), varargs...)
}

// OperateLeader mocks base method
func (m *MockMasterClient) OperateLeader(arg0 context.Context, arg1 *pb.OperateLeaderRequest, arg2 ...grpc.CallOption) (*pb.OperateLeaderResponse, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{arg0, arg1}
	for _, a := range arg2 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "OperateLeader", varargs...)
	ret0, _ := ret[0].(*pb.OperateLeaderResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// OperateLeader indicates an expected call of OperateLeader
func (mr *MockMasterClientMockRecorder) OperateLeader(arg0, arg1 interface{}, arg2 ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{arg0, arg1}, arg2...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "OperateLeader", reflect.TypeOf((*MockMasterClient)(nil).OperateLeader), varargs...)
}

// OperateSchema mocks base method
func (m *MockMasterClient) OperateSchema(arg0 context.Context, arg1 *pb.OperateSchemaRequest, arg2 ...grpc.CallOption) (*pb.OperateSchemaResponse, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{arg0, arg1}
	for _, a := range arg2 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "OperateSchema", varargs...)
	ret0, _ := ret[0].(*pb.OperateSchemaResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// OperateSchema indicates an expected call of OperateSchema
func (mr *MockMasterClientMockRecorder) OperateSchema(arg0, arg1 interface{}, arg2 ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{arg0, arg1}, arg2...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "OperateSchema", reflect.TypeOf((*MockMasterClient)(nil).OperateSchema), varargs...)
}

// OperateSource mocks base method
func (m *MockMasterClient) OperateSource(arg0 context.Context, arg1 *pb.OperateSourceRequest, arg2 ...grpc.CallOption) (*pb.OperateSourceResponse, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{arg0, arg1}
	for _, a := range arg2 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "OperateSource", varargs...)
	ret0, _ := ret[0].(*pb.OperateSourceResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// OperateSource indicates an expected call of OperateSource
func (mr *MockMasterClientMockRecorder) OperateSource(arg0, arg1 interface{}, arg2 ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{arg0, arg1}, arg2...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "OperateSource", reflect.TypeOf((*MockMasterClient)(nil).OperateSource), varargs...)
}

// OperateTask mocks base method
func (m *MockMasterClient) OperateTask(arg0 context.Context, arg1 *pb.OperateTaskRequest, arg2 ...grpc.CallOption) (*pb.OperateTaskResponse, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{arg0, arg1}
	for _, a := range arg2 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "OperateTask", varargs...)
	ret0, _ := ret[0].(*pb.OperateTaskResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// OperateTask indicates an expected call of OperateTask
func (mr *MockMasterClientMockRecorder) OperateTask(arg0, arg1 interface{}, arg2 ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{arg0, arg1}, arg2...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "OperateTask", reflect.TypeOf((*MockMasterClient)(nil).OperateTask), varargs...)
}

// OperateWorkerRelayTask mocks base method
func (m *MockMasterClient) OperateWorkerRelayTask(arg0 context.Context, arg1 *pb.OperateWorkerRelayRequest, arg2 ...grpc.CallOption) (*pb.OperateWorkerRelayResponse, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{arg0, arg1}
	for _, a := range arg2 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "OperateWorkerRelayTask", varargs...)
	ret0, _ := ret[0].(*pb.OperateWorkerRelayResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// OperateWorkerRelayTask indicates an expected call of OperateWorkerRelayTask
func (mr *MockMasterClientMockRecorder) OperateWorkerRelayTask(arg0, arg1 interface{}, arg2 ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{arg0, arg1}, arg2...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "OperateWorkerRelayTask", reflect.TypeOf((*MockMasterClient)(nil).OperateWorkerRelayTask), varargs...)
}

// PurgeWorkerRelay mocks base method
func (m *MockMasterClient) PurgeWorkerRelay(arg0 context.Context, arg1 *pb.PurgeWorkerRelayRequest, arg2 ...grpc.CallOption) (*pb.PurgeWorkerRelayResponse, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{arg0, arg1}
	for _, a := range arg2 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "PurgeWorkerRelay", varargs...)
	ret0, _ := ret[0].(*pb.PurgeWorkerRelayResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// PurgeWorkerRelay indicates an expected call of PurgeWorkerRelay
func (mr *MockMasterClientMockRecorder) PurgeWorkerRelay(arg0, arg1 interface{}, arg2 ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{arg0, arg1}, arg2...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "PurgeWorkerRelay", reflect.TypeOf((*MockMasterClient)(nil).PurgeWorkerRelay), varargs...)
}

// QueryError mocks base method
func (m *MockMasterClient) QueryError(arg0 context.Context, arg1 *pb.QueryErrorListRequest, arg2 ...grpc.CallOption) (*pb.QueryErrorListResponse, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{arg0, arg1}
	for _, a := range arg2 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "QueryError", varargs...)
	ret0, _ := ret[0].(*pb.QueryErrorListResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// QueryError indicates an expected call of QueryError
func (mr *MockMasterClientMockRecorder) QueryError(arg0, arg1 interface{}, arg2 ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{arg0, arg1}, arg2...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "QueryError", reflect.TypeOf((*MockMasterClient)(nil).QueryError), varargs...)
}

// QueryStatus mocks base method
func (m *MockMasterClient) QueryStatus(arg0 context.Context, arg1 *pb.QueryStatusListRequest, arg2 ...grpc.CallOption) (*pb.QueryStatusListResponse, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{arg0, arg1}
	for _, a := range arg2 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "QueryStatus", varargs...)
	ret0, _ := ret[0].(*pb.QueryStatusListResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// QueryStatus indicates an expected call of QueryStatus
func (mr *MockMasterClientMockRecorder) QueryStatus(arg0, arg1 interface{}, arg2 ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{arg0, arg1}, arg2...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "QueryStatus", reflect.TypeOf((*MockMasterClient)(nil).QueryStatus), varargs...)
}

// RegisterWorker mocks base method
func (m *MockMasterClient) RegisterWorker(arg0 context.Context, arg1 *pb.RegisterWorkerRequest, arg2 ...grpc.CallOption) (*pb.RegisterWorkerResponse, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{arg0, arg1}
	for _, a := range arg2 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "RegisterWorker", varargs...)
	ret0, _ := ret[0].(*pb.RegisterWorkerResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// RegisterWorker indicates an expected call of RegisterWorker
func (mr *MockMasterClientMockRecorder) RegisterWorker(arg0, arg1 interface{}, arg2 ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{arg0, arg1}, arg2...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RegisterWorker", reflect.TypeOf((*MockMasterClient)(nil).RegisterWorker), varargs...)
}

// ShowDDLLocks mocks base method
func (m *MockMasterClient) ShowDDLLocks(arg0 context.Context, arg1 *pb.ShowDDLLocksRequest, arg2 ...grpc.CallOption) (*pb.ShowDDLLocksResponse, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{arg0, arg1}
	for _, a := range arg2 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "ShowDDLLocks", varargs...)
	ret0, _ := ret[0].(*pb.ShowDDLLocksResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ShowDDLLocks indicates an expected call of ShowDDLLocks
func (mr *MockMasterClientMockRecorder) ShowDDLLocks(arg0, arg1 interface{}, arg2 ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{arg0, arg1}, arg2...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ShowDDLLocks", reflect.TypeOf((*MockMasterClient)(nil).ShowDDLLocks), varargs...)
}

// StartTask mocks base method
func (m *MockMasterClient) StartTask(arg0 context.Context, arg1 *pb.StartTaskRequest, arg2 ...grpc.CallOption) (*pb.StartTaskResponse, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{arg0, arg1}
	for _, a := range arg2 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "StartTask", varargs...)
	ret0, _ := ret[0].(*pb.StartTaskResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// StartTask indicates an expected call of StartTask
func (mr *MockMasterClientMockRecorder) StartTask(arg0, arg1 interface{}, arg2 ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{arg0, arg1}, arg2...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "StartTask", reflect.TypeOf((*MockMasterClient)(nil).StartTask), varargs...)
}

// SwitchWorkerRelayMaster mocks base method
func (m *MockMasterClient) SwitchWorkerRelayMaster(arg0 context.Context, arg1 *pb.SwitchWorkerRelayMasterRequest, arg2 ...grpc.CallOption) (*pb.SwitchWorkerRelayMasterResponse, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{arg0, arg1}
	for _, a := range arg2 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "SwitchWorkerRelayMaster", varargs...)
	ret0, _ := ret[0].(*pb.SwitchWorkerRelayMasterResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// SwitchWorkerRelayMaster indicates an expected call of SwitchWorkerRelayMaster
func (mr *MockMasterClientMockRecorder) SwitchWorkerRelayMaster(arg0, arg1 interface{}, arg2 ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{arg0, arg1}, arg2...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SwitchWorkerRelayMaster", reflect.TypeOf((*MockMasterClient)(nil).SwitchWorkerRelayMaster), varargs...)
}

// UnlockDDLLock mocks base method
func (m *MockMasterClient) UnlockDDLLock(arg0 context.Context, arg1 *pb.UnlockDDLLockRequest, arg2 ...grpc.CallOption) (*pb.UnlockDDLLockResponse, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{arg0, arg1}
	for _, a := range arg2 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "UnlockDDLLock", varargs...)
	ret0, _ := ret[0].(*pb.UnlockDDLLockResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// UnlockDDLLock indicates an expected call of UnlockDDLLock
func (mr *MockMasterClientMockRecorder) UnlockDDLLock(arg0, arg1 interface{}, arg2 ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{arg0, arg1}, arg2...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "UnlockDDLLock", reflect.TypeOf((*MockMasterClient)(nil).UnlockDDLLock), varargs...)
}

// UpdateMasterConfig mocks base method
func (m *MockMasterClient) UpdateMasterConfig(arg0 context.Context, arg1 *pb.UpdateMasterConfigRequest, arg2 ...grpc.CallOption) (*pb.UpdateMasterConfigResponse, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{arg0, arg1}
	for _, a := range arg2 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "UpdateMasterConfig", varargs...)
	ret0, _ := ret[0].(*pb.UpdateMasterConfigResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// UpdateMasterConfig indicates an expected call of UpdateMasterConfig
func (mr *MockMasterClientMockRecorder) UpdateMasterConfig(arg0, arg1 interface{}, arg2 ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{arg0, arg1}, arg2...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "UpdateMasterConfig", reflect.TypeOf((*MockMasterClient)(nil).UpdateMasterConfig), varargs...)
}

// UpdateTask mocks base method
func (m *MockMasterClient) UpdateTask(arg0 context.Context, arg1 *pb.UpdateTaskRequest, arg2 ...grpc.CallOption) (*pb.UpdateTaskResponse, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{arg0, arg1}
	for _, a := range arg2 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "UpdateTask", varargs...)
	ret0, _ := ret[0].(*pb.UpdateTaskResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// UpdateTask indicates an expected call of UpdateTask
func (mr *MockMasterClientMockRecorder) UpdateTask(arg0, arg1 interface{}, arg2 ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{arg0, arg1}, arg2...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "UpdateTask", reflect.TypeOf((*MockMasterClient)(nil).UpdateTask), varargs...)
}

// UpdateWorkerRelayConfig mocks base method
func (m *MockMasterClient) UpdateWorkerRelayConfig(arg0 context.Context, arg1 *pb.UpdateWorkerRelayConfigRequest, arg2 ...grpc.CallOption) (*pb.CommonWorkerResponse, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{arg0, arg1}
	for _, a := range arg2 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "UpdateWorkerRelayConfig", varargs...)
	ret0, _ := ret[0].(*pb.CommonWorkerResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// UpdateWorkerRelayConfig indicates an expected call of UpdateWorkerRelayConfig
func (mr *MockMasterClientMockRecorder) UpdateWorkerRelayConfig(arg0, arg1 interface{}, arg2 ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{arg0, arg1}, arg2...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "UpdateWorkerRelayConfig", reflect.TypeOf((*MockMasterClient)(nil).UpdateWorkerRelayConfig), varargs...)
}

// MockMasterServer is a mock of MasterServer interface
type MockMasterServer struct {
	ctrl     *gomock.Controller
	recorder *MockMasterServerMockRecorder
}

// MockMasterServerMockRecorder is the mock recorder for MockMasterServer
type MockMasterServerMockRecorder struct {
	mock *MockMasterServer
}

// NewMockMasterServer creates a new mock instance
func NewMockMasterServer(ctrl *gomock.Controller) *MockMasterServer {
	mock := &MockMasterServer{ctrl: ctrl}
	mock.recorder = &MockMasterServerMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use
func (m *MockMasterServer) EXPECT() *MockMasterServerMockRecorder {
	return m.recorder
}

// CheckTask mocks base method
func (m *MockMasterServer) CheckTask(arg0 context.Context, arg1 *pb.CheckTaskRequest) (*pb.CheckTaskResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "CheckTask", arg0, arg1)
	ret0, _ := ret[0].(*pb.CheckTaskResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// CheckTask indicates an expected call of CheckTask
func (mr *MockMasterServerMockRecorder) CheckTask(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CheckTask", reflect.TypeOf((*MockMasterServer)(nil).CheckTask), arg0, arg1)
}

// GetSubTaskCfg mocks base method
func (m *MockMasterServer) GetSubTaskCfg(arg0 context.Context, arg1 *pb.GetSubTaskCfgRequest) (*pb.GetSubTaskCfgResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetSubTaskCfg", arg0, arg1)
	ret0, _ := ret[0].(*pb.GetSubTaskCfgResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetSubTaskCfg indicates an expected call of GetSubTaskCfg
func (mr *MockMasterServerMockRecorder) GetSubTaskCfg(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetSubTaskCfg", reflect.TypeOf((*MockMasterServer)(nil).GetSubTaskCfg), arg0, arg1)
}

// GetTaskCfg mocks base method
func (m *MockMasterServer) GetTaskCfg(arg0 context.Context, arg1 *pb.GetTaskCfgRequest) (*pb.GetTaskCfgResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetTaskCfg", arg0, arg1)
	ret0, _ := ret[0].(*pb.GetTaskCfgResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetTaskCfg indicates an expected call of GetTaskCfg
func (mr *MockMasterServerMockRecorder) GetTaskCfg(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetTaskCfg", reflect.TypeOf((*MockMasterServer)(nil).GetTaskCfg), arg0, arg1)
}

// HandleSQLs mocks base method
func (m *MockMasterServer) HandleSQLs(arg0 context.Context, arg1 *pb.HandleSQLsRequest) (*pb.HandleSQLsResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "HandleSQLs", arg0, arg1)
	ret0, _ := ret[0].(*pb.HandleSQLsResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// HandleSQLs indicates an expected call of HandleSQLs
func (mr *MockMasterServerMockRecorder) HandleSQLs(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "HandleSQLs", reflect.TypeOf((*MockMasterServer)(nil).HandleSQLs), arg0, arg1)
}

// ListMember mocks base method
func (m *MockMasterServer) ListMember(arg0 context.Context, arg1 *pb.ListMemberRequest) (*pb.ListMemberResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ListMember", arg0, arg1)
	ret0, _ := ret[0].(*pb.ListMemberResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ListMember indicates an expected call of ListMember
func (mr *MockMasterServerMockRecorder) ListMember(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ListMember", reflect.TypeOf((*MockMasterServer)(nil).ListMember), arg0, arg1)
}

// MigrateWorkerRelay mocks base method
func (m *MockMasterServer) MigrateWorkerRelay(arg0 context.Context, arg1 *pb.MigrateWorkerRelayRequest) (*pb.CommonWorkerResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "MigrateWorkerRelay", arg0, arg1)
	ret0, _ := ret[0].(*pb.CommonWorkerResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// MigrateWorkerRelay indicates an expected call of MigrateWorkerRelay
func (mr *MockMasterServerMockRecorder) MigrateWorkerRelay(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "MigrateWorkerRelay", reflect.TypeOf((*MockMasterServer)(nil).MigrateWorkerRelay), arg0, arg1)
}

// OfflineMember mocks base method
func (m *MockMasterServer) OfflineMember(arg0 context.Context, arg1 *pb.OfflineMemberRequest) (*pb.OfflineMemberResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "OfflineMember", arg0, arg1)
	ret0, _ := ret[0].(*pb.OfflineMemberResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// OfflineMember indicates an expected call of OfflineMember
func (mr *MockMasterServerMockRecorder) OfflineMember(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "OfflineMember", reflect.TypeOf((*MockMasterServer)(nil).OfflineMember), arg0, arg1)
}

// OperateLeader mocks base method
func (m *MockMasterServer) OperateLeader(arg0 context.Context, arg1 *pb.OperateLeaderRequest) (*pb.OperateLeaderResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "OperateLeader", arg0, arg1)
	ret0, _ := ret[0].(*pb.OperateLeaderResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// OperateLeader indicates an expected call of OperateLeader
func (mr *MockMasterServerMockRecorder) OperateLeader(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "OperateLeader", reflect.TypeOf((*MockMasterServer)(nil).OperateLeader), arg0, arg1)
}

// OperateSchema mocks base method
func (m *MockMasterServer) OperateSchema(arg0 context.Context, arg1 *pb.OperateSchemaRequest) (*pb.OperateSchemaResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "OperateSchema", arg0, arg1)
	ret0, _ := ret[0].(*pb.OperateSchemaResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// OperateSchema indicates an expected call of OperateSchema
func (mr *MockMasterServerMockRecorder) OperateSchema(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "OperateSchema", reflect.TypeOf((*MockMasterServer)(nil).OperateSchema), arg0, arg1)
}

// OperateSource mocks base method
func (m *MockMasterServer) OperateSource(arg0 context.Context, arg1 *pb.OperateSourceRequest) (*pb.OperateSourceResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "OperateSource", arg0, arg1)
	ret0, _ := ret[0].(*pb.OperateSourceResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// OperateSource indicates an expected call of OperateSource
func (mr *MockMasterServerMockRecorder) OperateSource(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "OperateSource", reflect.TypeOf((*MockMasterServer)(nil).OperateSource), arg0, arg1)
}

// OperateTask mocks base method
func (m *MockMasterServer) OperateTask(arg0 context.Context, arg1 *pb.OperateTaskRequest) (*pb.OperateTaskResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "OperateTask", arg0, arg1)
	ret0, _ := ret[0].(*pb.OperateTaskResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// OperateTask indicates an expected call of OperateTask
func (mr *MockMasterServerMockRecorder) OperateTask(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "OperateTask", reflect.TypeOf((*MockMasterServer)(nil).OperateTask), arg0, arg1)
}

// OperateWorkerRelayTask mocks base method
func (m *MockMasterServer) OperateWorkerRelayTask(arg0 context.Context, arg1 *pb.OperateWorkerRelayRequest) (*pb.OperateWorkerRelayResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "OperateWorkerRelayTask", arg0, arg1)
	ret0, _ := ret[0].(*pb.OperateWorkerRelayResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// OperateWorkerRelayTask indicates an expected call of OperateWorkerRelayTask
func (mr *MockMasterServerMockRecorder) OperateWorkerRelayTask(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "OperateWorkerRelayTask", reflect.TypeOf((*MockMasterServer)(nil).OperateWorkerRelayTask), arg0, arg1)
}

// PurgeWorkerRelay mocks base method
func (m *MockMasterServer) PurgeWorkerRelay(arg0 context.Context, arg1 *pb.PurgeWorkerRelayRequest) (*pb.PurgeWorkerRelayResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "PurgeWorkerRelay", arg0, arg1)
	ret0, _ := ret[0].(*pb.PurgeWorkerRelayResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// PurgeWorkerRelay indicates an expected call of PurgeWorkerRelay
func (mr *MockMasterServerMockRecorder) PurgeWorkerRelay(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "PurgeWorkerRelay", reflect.TypeOf((*MockMasterServer)(nil).PurgeWorkerRelay), arg0, arg1)
}

// QueryError mocks base method
func (m *MockMasterServer) QueryError(arg0 context.Context, arg1 *pb.QueryErrorListRequest) (*pb.QueryErrorListResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "QueryError", arg0, arg1)
	ret0, _ := ret[0].(*pb.QueryErrorListResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// QueryError indicates an expected call of QueryError
func (mr *MockMasterServerMockRecorder) QueryError(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "QueryError", reflect.TypeOf((*MockMasterServer)(nil).QueryError), arg0, arg1)
}

// QueryStatus mocks base method
func (m *MockMasterServer) QueryStatus(arg0 context.Context, arg1 *pb.QueryStatusListRequest) (*pb.QueryStatusListResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "QueryStatus", arg0, arg1)
	ret0, _ := ret[0].(*pb.QueryStatusListResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// QueryStatus indicates an expected call of QueryStatus
func (mr *MockMasterServerMockRecorder) QueryStatus(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "QueryStatus", reflect.TypeOf((*MockMasterServer)(nil).QueryStatus), arg0, arg1)
}

// RegisterWorker mocks base method
func (m *MockMasterServer) RegisterWorker(arg0 context.Context, arg1 *pb.RegisterWorkerRequest) (*pb.RegisterWorkerResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "RegisterWorker", arg0, arg1)
	ret0, _ := ret[0].(*pb.RegisterWorkerResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// RegisterWorker indicates an expected call of RegisterWorker
func (mr *MockMasterServerMockRecorder) RegisterWorker(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RegisterWorker", reflect.TypeOf((*MockMasterServer)(nil).RegisterWorker), arg0, arg1)
}

// ShowDDLLocks mocks base method
func (m *MockMasterServer) ShowDDLLocks(arg0 context.Context, arg1 *pb.ShowDDLLocksRequest) (*pb.ShowDDLLocksResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ShowDDLLocks", arg0, arg1)
	ret0, _ := ret[0].(*pb.ShowDDLLocksResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ShowDDLLocks indicates an expected call of ShowDDLLocks
func (mr *MockMasterServerMockRecorder) ShowDDLLocks(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ShowDDLLocks", reflect.TypeOf((*MockMasterServer)(nil).ShowDDLLocks), arg0, arg1)
}

// StartTask mocks base method
func (m *MockMasterServer) StartTask(arg0 context.Context, arg1 *pb.StartTaskRequest) (*pb.StartTaskResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "StartTask", arg0, arg1)
	ret0, _ := ret[0].(*pb.StartTaskResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// StartTask indicates an expected call of StartTask
func (mr *MockMasterServerMockRecorder) StartTask(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "StartTask", reflect.TypeOf((*MockMasterServer)(nil).StartTask), arg0, arg1)
}

// SwitchWorkerRelayMaster mocks base method
func (m *MockMasterServer) SwitchWorkerRelayMaster(arg0 context.Context, arg1 *pb.SwitchWorkerRelayMasterRequest) (*pb.SwitchWorkerRelayMasterResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "SwitchWorkerRelayMaster", arg0, arg1)
	ret0, _ := ret[0].(*pb.SwitchWorkerRelayMasterResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// SwitchWorkerRelayMaster indicates an expected call of SwitchWorkerRelayMaster
func (mr *MockMasterServerMockRecorder) SwitchWorkerRelayMaster(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SwitchWorkerRelayMaster", reflect.TypeOf((*MockMasterServer)(nil).SwitchWorkerRelayMaster), arg0, arg1)
}

// UnlockDDLLock mocks base method
func (m *MockMasterServer) UnlockDDLLock(arg0 context.Context, arg1 *pb.UnlockDDLLockRequest) (*pb.UnlockDDLLockResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "UnlockDDLLock", arg0, arg1)
	ret0, _ := ret[0].(*pb.UnlockDDLLockResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// UnlockDDLLock indicates an expected call of UnlockDDLLock
func (mr *MockMasterServerMockRecorder) UnlockDDLLock(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "UnlockDDLLock", reflect.TypeOf((*MockMasterServer)(nil).UnlockDDLLock), arg0, arg1)
}

// UpdateMasterConfig mocks base method
func (m *MockMasterServer) UpdateMasterConfig(arg0 context.Context, arg1 *pb.UpdateMasterConfigRequest) (*pb.UpdateMasterConfigResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "UpdateMasterConfig", arg0, arg1)
	ret0, _ := ret[0].(*pb.UpdateMasterConfigResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// UpdateMasterConfig indicates an expected call of UpdateMasterConfig
func (mr *MockMasterServerMockRecorder) UpdateMasterConfig(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "UpdateMasterConfig", reflect.TypeOf((*MockMasterServer)(nil).UpdateMasterConfig), arg0, arg1)
}

// UpdateTask mocks base method
func (m *MockMasterServer) UpdateTask(arg0 context.Context, arg1 *pb.UpdateTaskRequest) (*pb.UpdateTaskResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "UpdateTask", arg0, arg1)
	ret0, _ := ret[0].(*pb.UpdateTaskResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// UpdateTask indicates an expected call of UpdateTask
func (mr *MockMasterServerMockRecorder) UpdateTask(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "UpdateTask", reflect.TypeOf((*MockMasterServer)(nil).UpdateTask), arg0, arg1)
}

// UpdateWorkerRelayConfig mocks base method
func (m *MockMasterServer) UpdateWorkerRelayConfig(arg0 context.Context, arg1 *pb.UpdateWorkerRelayConfigRequest) (*pb.CommonWorkerResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "UpdateWorkerRelayConfig", arg0, arg1)
	ret0, _ := ret[0].(*pb.CommonWorkerResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// UpdateWorkerRelayConfig indicates an expected call of UpdateWorkerRelayConfig
func (mr *MockMasterServerMockRecorder) UpdateWorkerRelayConfig(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "UpdateWorkerRelayConfig", reflect.TypeOf((*MockMasterServer)(nil).UpdateWorkerRelayConfig), arg0, arg1)
}
