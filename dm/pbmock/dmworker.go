// Code generated by MockGen. DO NOT EDIT.
// Source: github.com/pingcap/dm/dm/pb (interfaces: WorkerClient,Worker_FetchDDLInfoClient,WorkerServer,Worker_FetchDDLInfoServer)

// Package pbmock is a generated GoMock package.
package pbmock

import (
	context "context"
	gomock "github.com/golang/mock/gomock"
	pb "github.com/pingcap/dm/dm/pb"
	grpc "google.golang.org/grpc"
	metadata "google.golang.org/grpc/metadata"
	reflect "reflect"
)

// MockWorkerClient is a mock of WorkerClient interface
type MockWorkerClient struct {
	ctrl     *gomock.Controller
	recorder *MockWorkerClientMockRecorder
}

// MockWorkerClientMockRecorder is the mock recorder for MockWorkerClient
type MockWorkerClientMockRecorder struct {
	mock *MockWorkerClient
}

// NewMockWorkerClient creates a new mock instance
func NewMockWorkerClient(ctrl *gomock.Controller) *MockWorkerClient {
	mock := &MockWorkerClient{ctrl: ctrl}
	mock.recorder = &MockWorkerClientMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use
func (m *MockWorkerClient) EXPECT() *MockWorkerClientMockRecorder {
	return m.recorder
}

// BreakDDLLock mocks base method
func (m *MockWorkerClient) BreakDDLLock(arg0 context.Context, arg1 *pb.BreakDDLLockRequest, arg2 ...grpc.CallOption) (*pb.CommonWorkerResponse, error) {
	varargs := []interface{}{arg0, arg1}
	for _, a := range arg2 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "BreakDDLLock", varargs...)
	ret0, _ := ret[0].(*pb.CommonWorkerResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// BreakDDLLock indicates an expected call of BreakDDLLock
func (mr *MockWorkerClientMockRecorder) BreakDDLLock(arg0, arg1 interface{}, arg2 ...interface{}) *gomock.Call {
	varargs := append([]interface{}{arg0, arg1}, arg2...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "BreakDDLLock", reflect.TypeOf((*MockWorkerClient)(nil).BreakDDLLock), varargs...)
}

// ExecuteDDL mocks base method
func (m *MockWorkerClient) ExecuteDDL(arg0 context.Context, arg1 *pb.ExecDDLRequest, arg2 ...grpc.CallOption) (*pb.CommonWorkerResponse, error) {
	varargs := []interface{}{arg0, arg1}
	for _, a := range arg2 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "ExecuteDDL", varargs...)
	ret0, _ := ret[0].(*pb.CommonWorkerResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ExecuteDDL indicates an expected call of ExecuteDDL
func (mr *MockWorkerClientMockRecorder) ExecuteDDL(arg0, arg1 interface{}, arg2 ...interface{}) *gomock.Call {
	varargs := append([]interface{}{arg0, arg1}, arg2...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ExecuteDDL", reflect.TypeOf((*MockWorkerClient)(nil).ExecuteDDL), varargs...)
}

// FetchDDLInfo mocks base method
func (m *MockWorkerClient) FetchDDLInfo(arg0 context.Context, arg1 ...grpc.CallOption) (pb.Worker_FetchDDLInfoClient, error) {
	varargs := []interface{}{arg0}
	for _, a := range arg1 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "FetchDDLInfo", varargs...)
	ret0, _ := ret[0].(pb.Worker_FetchDDLInfoClient)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// FetchDDLInfo indicates an expected call of FetchDDLInfo
func (mr *MockWorkerClientMockRecorder) FetchDDLInfo(arg0 interface{}, arg1 ...interface{}) *gomock.Call {
	varargs := append([]interface{}{arg0}, arg1...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "FetchDDLInfo", reflect.TypeOf((*MockWorkerClient)(nil).FetchDDLInfo), varargs...)
}

// HandleSQLs mocks base method
func (m *MockWorkerClient) HandleSQLs(arg0 context.Context, arg1 *pb.HandleSubTaskSQLsRequest, arg2 ...grpc.CallOption) (*pb.CommonWorkerResponse, error) {
	varargs := []interface{}{arg0, arg1}
	for _, a := range arg2 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "HandleSQLs", varargs...)
	ret0, _ := ret[0].(*pb.CommonWorkerResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// HandleSQLs indicates an expected call of HandleSQLs
func (mr *MockWorkerClientMockRecorder) HandleSQLs(arg0, arg1 interface{}, arg2 ...interface{}) *gomock.Call {
	varargs := append([]interface{}{arg0, arg1}, arg2...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "HandleSQLs", reflect.TypeOf((*MockWorkerClient)(nil).HandleSQLs), varargs...)
}

// MigrateRelay mocks base method
func (m *MockWorkerClient) MigrateRelay(arg0 context.Context, arg1 *pb.MigrateRelayRequest, arg2 ...grpc.CallOption) (*pb.CommonWorkerResponse, error) {
	varargs := []interface{}{arg0, arg1}
	for _, a := range arg2 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "MigrateRelay", varargs...)
	ret0, _ := ret[0].(*pb.CommonWorkerResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// MigrateRelay indicates an expected call of MigrateRelay
func (mr *MockWorkerClientMockRecorder) MigrateRelay(arg0, arg1 interface{}, arg2 ...interface{}) *gomock.Call {
	varargs := append([]interface{}{arg0, arg1}, arg2...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "MigrateRelay", reflect.TypeOf((*MockWorkerClient)(nil).MigrateRelay), varargs...)
}

// OperateRelay mocks base method
func (m *MockWorkerClient) OperateRelay(arg0 context.Context, arg1 *pb.OperateRelayRequest, arg2 ...grpc.CallOption) (*pb.OperateRelayResponse, error) {
	varargs := []interface{}{arg0, arg1}
	for _, a := range arg2 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "OperateRelay", varargs...)
	ret0, _ := ret[0].(*pb.OperateRelayResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// OperateRelay indicates an expected call of OperateRelay
func (mr *MockWorkerClientMockRecorder) OperateRelay(arg0, arg1 interface{}, arg2 ...interface{}) *gomock.Call {
	varargs := append([]interface{}{arg0, arg1}, arg2...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "OperateRelay", reflect.TypeOf((*MockWorkerClient)(nil).OperateRelay), varargs...)
}

// OperateSubTask mocks base method
func (m *MockWorkerClient) OperateSubTask(arg0 context.Context, arg1 *pb.OperateSubTaskRequest, arg2 ...grpc.CallOption) (*pb.OperateSubTaskResponse, error) {
	varargs := []interface{}{arg0, arg1}
	for _, a := range arg2 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "OperateSubTask", varargs...)
	ret0, _ := ret[0].(*pb.OperateSubTaskResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// OperateSubTask indicates an expected call of OperateSubTask
func (mr *MockWorkerClientMockRecorder) OperateSubTask(arg0, arg1 interface{}, arg2 ...interface{}) *gomock.Call {
	varargs := append([]interface{}{arg0, arg1}, arg2...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "OperateSubTask", reflect.TypeOf((*MockWorkerClient)(nil).OperateSubTask), varargs...)
}

// PurgeRelay mocks base method
func (m *MockWorkerClient) PurgeRelay(arg0 context.Context, arg1 *pb.PurgeRelayRequest, arg2 ...grpc.CallOption) (*pb.CommonWorkerResponse, error) {
	varargs := []interface{}{arg0, arg1}
	for _, a := range arg2 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "PurgeRelay", varargs...)
	ret0, _ := ret[0].(*pb.CommonWorkerResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// PurgeRelay indicates an expected call of PurgeRelay
func (mr *MockWorkerClientMockRecorder) PurgeRelay(arg0, arg1 interface{}, arg2 ...interface{}) *gomock.Call {
	varargs := append([]interface{}{arg0, arg1}, arg2...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "PurgeRelay", reflect.TypeOf((*MockWorkerClient)(nil).PurgeRelay), varargs...)
}

// QueryError mocks base method
func (m *MockWorkerClient) QueryError(arg0 context.Context, arg1 *pb.QueryErrorRequest, arg2 ...grpc.CallOption) (*pb.QueryErrorResponse, error) {
	varargs := []interface{}{arg0, arg1}
	for _, a := range arg2 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "QueryError", varargs...)
	ret0, _ := ret[0].(*pb.QueryErrorResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// QueryError indicates an expected call of QueryError
func (mr *MockWorkerClientMockRecorder) QueryError(arg0, arg1 interface{}, arg2 ...interface{}) *gomock.Call {
	varargs := append([]interface{}{arg0, arg1}, arg2...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "QueryError", reflect.TypeOf((*MockWorkerClient)(nil).QueryError), varargs...)
}

// QueryStatus mocks base method
func (m *MockWorkerClient) QueryStatus(arg0 context.Context, arg1 *pb.QueryStatusRequest, arg2 ...grpc.CallOption) (*pb.QueryStatusResponse, error) {
	varargs := []interface{}{arg0, arg1}
	for _, a := range arg2 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "QueryStatus", varargs...)
	ret0, _ := ret[0].(*pb.QueryStatusResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// QueryStatus indicates an expected call of QueryStatus
func (mr *MockWorkerClientMockRecorder) QueryStatus(arg0, arg1 interface{}, arg2 ...interface{}) *gomock.Call {
	varargs := append([]interface{}{arg0, arg1}, arg2...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "QueryStatus", reflect.TypeOf((*MockWorkerClient)(nil).QueryStatus), varargs...)
}

// QueryWorkerConfig mocks base method
func (m *MockWorkerClient) QueryWorkerConfig(arg0 context.Context, arg1 *pb.QueryWorkerConfigRequest, arg2 ...grpc.CallOption) (*pb.QueryWorkerConfigResponse, error) {
	varargs := []interface{}{arg0, arg1}
	for _, a := range arg2 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "QueryWorkerConfig", varargs...)
	ret0, _ := ret[0].(*pb.QueryWorkerConfigResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// QueryWorkerConfig indicates an expected call of QueryWorkerConfig
func (mr *MockWorkerClientMockRecorder) QueryWorkerConfig(arg0, arg1 interface{}, arg2 ...interface{}) *gomock.Call {
	varargs := append([]interface{}{arg0, arg1}, arg2...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "QueryWorkerConfig", reflect.TypeOf((*MockWorkerClient)(nil).QueryWorkerConfig), varargs...)
}

// StartSubTask mocks base method
func (m *MockWorkerClient) StartSubTask(arg0 context.Context, arg1 *pb.StartSubTaskRequest, arg2 ...grpc.CallOption) (*pb.CommonWorkerResponse, error) {
	varargs := []interface{}{arg0, arg1}
	for _, a := range arg2 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "StartSubTask", varargs...)
	ret0, _ := ret[0].(*pb.CommonWorkerResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// StartSubTask indicates an expected call of StartSubTask
func (mr *MockWorkerClientMockRecorder) StartSubTask(arg0, arg1 interface{}, arg2 ...interface{}) *gomock.Call {
	varargs := append([]interface{}{arg0, arg1}, arg2...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "StartSubTask", reflect.TypeOf((*MockWorkerClient)(nil).StartSubTask), varargs...)
}

// SwitchRelayMaster mocks base method
func (m *MockWorkerClient) SwitchRelayMaster(arg0 context.Context, arg1 *pb.SwitchRelayMasterRequest, arg2 ...grpc.CallOption) (*pb.CommonWorkerResponse, error) {
	varargs := []interface{}{arg0, arg1}
	for _, a := range arg2 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "SwitchRelayMaster", varargs...)
	ret0, _ := ret[0].(*pb.CommonWorkerResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// SwitchRelayMaster indicates an expected call of SwitchRelayMaster
func (mr *MockWorkerClientMockRecorder) SwitchRelayMaster(arg0, arg1 interface{}, arg2 ...interface{}) *gomock.Call {
	varargs := append([]interface{}{arg0, arg1}, arg2...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SwitchRelayMaster", reflect.TypeOf((*MockWorkerClient)(nil).SwitchRelayMaster), varargs...)
}

// UpdateRelayConfig mocks base method
func (m *MockWorkerClient) UpdateRelayConfig(arg0 context.Context, arg1 *pb.UpdateRelayRequest, arg2 ...grpc.CallOption) (*pb.CommonWorkerResponse, error) {
	varargs := []interface{}{arg0, arg1}
	for _, a := range arg2 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "UpdateRelayConfig", varargs...)
	ret0, _ := ret[0].(*pb.CommonWorkerResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// UpdateRelayConfig indicates an expected call of UpdateRelayConfig
func (mr *MockWorkerClientMockRecorder) UpdateRelayConfig(arg0, arg1 interface{}, arg2 ...interface{}) *gomock.Call {
	varargs := append([]interface{}{arg0, arg1}, arg2...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "UpdateRelayConfig", reflect.TypeOf((*MockWorkerClient)(nil).UpdateRelayConfig), varargs...)
}

// UpdateSubTask mocks base method
func (m *MockWorkerClient) UpdateSubTask(arg0 context.Context, arg1 *pb.UpdateSubTaskRequest, arg2 ...grpc.CallOption) (*pb.CommonWorkerResponse, error) {
	varargs := []interface{}{arg0, arg1}
	for _, a := range arg2 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "UpdateSubTask", varargs...)
	ret0, _ := ret[0].(*pb.CommonWorkerResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// UpdateSubTask indicates an expected call of UpdateSubTask
func (mr *MockWorkerClientMockRecorder) UpdateSubTask(arg0, arg1 interface{}, arg2 ...interface{}) *gomock.Call {
	varargs := append([]interface{}{arg0, arg1}, arg2...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "UpdateSubTask", reflect.TypeOf((*MockWorkerClient)(nil).UpdateSubTask), varargs...)
}

// MockWorker_FetchDDLInfoClient is a mock of Worker_FetchDDLInfoClient interface
type MockWorker_FetchDDLInfoClient struct {
	ctrl     *gomock.Controller
	recorder *MockWorker_FetchDDLInfoClientMockRecorder
}

// MockWorker_FetchDDLInfoClientMockRecorder is the mock recorder for MockWorker_FetchDDLInfoClient
type MockWorker_FetchDDLInfoClientMockRecorder struct {
	mock *MockWorker_FetchDDLInfoClient
}

// NewMockWorker_FetchDDLInfoClient creates a new mock instance
func NewMockWorker_FetchDDLInfoClient(ctrl *gomock.Controller) *MockWorker_FetchDDLInfoClient {
	mock := &MockWorker_FetchDDLInfoClient{ctrl: ctrl}
	mock.recorder = &MockWorker_FetchDDLInfoClientMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use
func (m *MockWorker_FetchDDLInfoClient) EXPECT() *MockWorker_FetchDDLInfoClientMockRecorder {
	return m.recorder
}

// CloseSend mocks base method
func (m *MockWorker_FetchDDLInfoClient) CloseSend() error {
	ret := m.ctrl.Call(m, "CloseSend")
	ret0, _ := ret[0].(error)
	return ret0
}

// CloseSend indicates an expected call of CloseSend
func (mr *MockWorker_FetchDDLInfoClientMockRecorder) CloseSend() *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CloseSend", reflect.TypeOf((*MockWorker_FetchDDLInfoClient)(nil).CloseSend))
}

// Context mocks base method
func (m *MockWorker_FetchDDLInfoClient) Context() context.Context {
	ret := m.ctrl.Call(m, "Context")
	ret0, _ := ret[0].(context.Context)
	return ret0
}

// Context indicates an expected call of Context
func (mr *MockWorker_FetchDDLInfoClientMockRecorder) Context() *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Context", reflect.TypeOf((*MockWorker_FetchDDLInfoClient)(nil).Context))
}

// Header mocks base method
func (m *MockWorker_FetchDDLInfoClient) Header() (metadata.MD, error) {
	ret := m.ctrl.Call(m, "Header")
	ret0, _ := ret[0].(metadata.MD)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Header indicates an expected call of Header
func (mr *MockWorker_FetchDDLInfoClientMockRecorder) Header() *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Header", reflect.TypeOf((*MockWorker_FetchDDLInfoClient)(nil).Header))
}

// Recv mocks base method
func (m *MockWorker_FetchDDLInfoClient) Recv() (*pb.DDLInfo, error) {
	ret := m.ctrl.Call(m, "Recv")
	ret0, _ := ret[0].(*pb.DDLInfo)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Recv indicates an expected call of Recv
func (mr *MockWorker_FetchDDLInfoClientMockRecorder) Recv() *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Recv", reflect.TypeOf((*MockWorker_FetchDDLInfoClient)(nil).Recv))
}

// RecvMsg mocks base method
func (m *MockWorker_FetchDDLInfoClient) RecvMsg(arg0 interface{}) error {
	ret := m.ctrl.Call(m, "RecvMsg", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// RecvMsg indicates an expected call of RecvMsg
func (mr *MockWorker_FetchDDLInfoClientMockRecorder) RecvMsg(arg0 interface{}) *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RecvMsg", reflect.TypeOf((*MockWorker_FetchDDLInfoClient)(nil).RecvMsg), arg0)
}

// Send mocks base method
func (m *MockWorker_FetchDDLInfoClient) Send(arg0 *pb.DDLLockInfo) error {
	ret := m.ctrl.Call(m, "Send", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// Send indicates an expected call of Send
func (mr *MockWorker_FetchDDLInfoClientMockRecorder) Send(arg0 interface{}) *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Send", reflect.TypeOf((*MockWorker_FetchDDLInfoClient)(nil).Send), arg0)
}

// SendMsg mocks base method
func (m *MockWorker_FetchDDLInfoClient) SendMsg(arg0 interface{}) error {
	ret := m.ctrl.Call(m, "SendMsg", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// SendMsg indicates an expected call of SendMsg
func (mr *MockWorker_FetchDDLInfoClientMockRecorder) SendMsg(arg0 interface{}) *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SendMsg", reflect.TypeOf((*MockWorker_FetchDDLInfoClient)(nil).SendMsg), arg0)
}

// Trailer mocks base method
func (m *MockWorker_FetchDDLInfoClient) Trailer() metadata.MD {
	ret := m.ctrl.Call(m, "Trailer")
	ret0, _ := ret[0].(metadata.MD)
	return ret0
}

// Trailer indicates an expected call of Trailer
func (mr *MockWorker_FetchDDLInfoClientMockRecorder) Trailer() *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Trailer", reflect.TypeOf((*MockWorker_FetchDDLInfoClient)(nil).Trailer))
}

// MockWorkerServer is a mock of WorkerServer interface
type MockWorkerServer struct {
	ctrl     *gomock.Controller
	recorder *MockWorkerServerMockRecorder
}

// MockWorkerServerMockRecorder is the mock recorder for MockWorkerServer
type MockWorkerServerMockRecorder struct {
	mock *MockWorkerServer
}

// NewMockWorkerServer creates a new mock instance
func NewMockWorkerServer(ctrl *gomock.Controller) *MockWorkerServer {
	mock := &MockWorkerServer{ctrl: ctrl}
	mock.recorder = &MockWorkerServerMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use
func (m *MockWorkerServer) EXPECT() *MockWorkerServerMockRecorder {
	return m.recorder
}

// BreakDDLLock mocks base method
func (m *MockWorkerServer) BreakDDLLock(arg0 context.Context, arg1 *pb.BreakDDLLockRequest) (*pb.CommonWorkerResponse, error) {
	ret := m.ctrl.Call(m, "BreakDDLLock", arg0, arg1)
	ret0, _ := ret[0].(*pb.CommonWorkerResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// BreakDDLLock indicates an expected call of BreakDDLLock
func (mr *MockWorkerServerMockRecorder) BreakDDLLock(arg0, arg1 interface{}) *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "BreakDDLLock", reflect.TypeOf((*MockWorkerServer)(nil).BreakDDLLock), arg0, arg1)
}

// ExecuteDDL mocks base method
func (m *MockWorkerServer) ExecuteDDL(arg0 context.Context, arg1 *pb.ExecDDLRequest) (*pb.CommonWorkerResponse, error) {
	ret := m.ctrl.Call(m, "ExecuteDDL", arg0, arg1)
	ret0, _ := ret[0].(*pb.CommonWorkerResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ExecuteDDL indicates an expected call of ExecuteDDL
func (mr *MockWorkerServerMockRecorder) ExecuteDDL(arg0, arg1 interface{}) *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ExecuteDDL", reflect.TypeOf((*MockWorkerServer)(nil).ExecuteDDL), arg0, arg1)
}

// FetchDDLInfo mocks base method
func (m *MockWorkerServer) FetchDDLInfo(arg0 pb.Worker_FetchDDLInfoServer) error {
	ret := m.ctrl.Call(m, "FetchDDLInfo", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// FetchDDLInfo indicates an expected call of FetchDDLInfo
func (mr *MockWorkerServerMockRecorder) FetchDDLInfo(arg0 interface{}) *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "FetchDDLInfo", reflect.TypeOf((*MockWorkerServer)(nil).FetchDDLInfo), arg0)
}

// HandleSQLs mocks base method
func (m *MockWorkerServer) HandleSQLs(arg0 context.Context, arg1 *pb.HandleSubTaskSQLsRequest) (*pb.CommonWorkerResponse, error) {
	ret := m.ctrl.Call(m, "HandleSQLs", arg0, arg1)
	ret0, _ := ret[0].(*pb.CommonWorkerResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// HandleSQLs indicates an expected call of HandleSQLs
func (mr *MockWorkerServerMockRecorder) HandleSQLs(arg0, arg1 interface{}) *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "HandleSQLs", reflect.TypeOf((*MockWorkerServer)(nil).HandleSQLs), arg0, arg1)
}

// MigrateRelay mocks base method
func (m *MockWorkerServer) MigrateRelay(arg0 context.Context, arg1 *pb.MigrateRelayRequest) (*pb.CommonWorkerResponse, error) {
	ret := m.ctrl.Call(m, "MigrateRelay", arg0, arg1)
	ret0, _ := ret[0].(*pb.CommonWorkerResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// MigrateRelay indicates an expected call of MigrateRelay
func (mr *MockWorkerServerMockRecorder) MigrateRelay(arg0, arg1 interface{}) *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "MigrateRelay", reflect.TypeOf((*MockWorkerServer)(nil).MigrateRelay), arg0, arg1)
}

// OperateRelay mocks base method
func (m *MockWorkerServer) OperateRelay(arg0 context.Context, arg1 *pb.OperateRelayRequest) (*pb.OperateRelayResponse, error) {
	ret := m.ctrl.Call(m, "OperateRelay", arg0, arg1)
	ret0, _ := ret[0].(*pb.OperateRelayResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// OperateRelay indicates an expected call of OperateRelay
func (mr *MockWorkerServerMockRecorder) OperateRelay(arg0, arg1 interface{}) *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "OperateRelay", reflect.TypeOf((*MockWorkerServer)(nil).OperateRelay), arg0, arg1)
}

// OperateSubTask mocks base method
func (m *MockWorkerServer) OperateSubTask(arg0 context.Context, arg1 *pb.OperateSubTaskRequest) (*pb.OperateSubTaskResponse, error) {
	ret := m.ctrl.Call(m, "OperateSubTask", arg0, arg1)
	ret0, _ := ret[0].(*pb.OperateSubTaskResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// OperateSubTask indicates an expected call of OperateSubTask
func (mr *MockWorkerServerMockRecorder) OperateSubTask(arg0, arg1 interface{}) *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "OperateSubTask", reflect.TypeOf((*MockWorkerServer)(nil).OperateSubTask), arg0, arg1)
}

// PurgeRelay mocks base method
func (m *MockWorkerServer) PurgeRelay(arg0 context.Context, arg1 *pb.PurgeRelayRequest) (*pb.CommonWorkerResponse, error) {
	ret := m.ctrl.Call(m, "PurgeRelay", arg0, arg1)
	ret0, _ := ret[0].(*pb.CommonWorkerResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// PurgeRelay indicates an expected call of PurgeRelay
func (mr *MockWorkerServerMockRecorder) PurgeRelay(arg0, arg1 interface{}) *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "PurgeRelay", reflect.TypeOf((*MockWorkerServer)(nil).PurgeRelay), arg0, arg1)
}

// QueryError mocks base method
func (m *MockWorkerServer) QueryError(arg0 context.Context, arg1 *pb.QueryErrorRequest) (*pb.QueryErrorResponse, error) {
	ret := m.ctrl.Call(m, "QueryError", arg0, arg1)
	ret0, _ := ret[0].(*pb.QueryErrorResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// QueryError indicates an expected call of QueryError
func (mr *MockWorkerServerMockRecorder) QueryError(arg0, arg1 interface{}) *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "QueryError", reflect.TypeOf((*MockWorkerServer)(nil).QueryError), arg0, arg1)
}

// QueryStatus mocks base method
func (m *MockWorkerServer) QueryStatus(arg0 context.Context, arg1 *pb.QueryStatusRequest) (*pb.QueryStatusResponse, error) {
	ret := m.ctrl.Call(m, "QueryStatus", arg0, arg1)
	ret0, _ := ret[0].(*pb.QueryStatusResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// QueryStatus indicates an expected call of QueryStatus
func (mr *MockWorkerServerMockRecorder) QueryStatus(arg0, arg1 interface{}) *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "QueryStatus", reflect.TypeOf((*MockWorkerServer)(nil).QueryStatus), arg0, arg1)
}

// QueryWorkerConfig mocks base method
func (m *MockWorkerServer) QueryWorkerConfig(arg0 context.Context, arg1 *pb.QueryWorkerConfigRequest) (*pb.QueryWorkerConfigResponse, error) {
	ret := m.ctrl.Call(m, "QueryWorkerConfig", arg0, arg1)
	ret0, _ := ret[0].(*pb.QueryWorkerConfigResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// QueryWorkerConfig indicates an expected call of QueryWorkerConfig
func (mr *MockWorkerServerMockRecorder) QueryWorkerConfig(arg0, arg1 interface{}) *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "QueryWorkerConfig", reflect.TypeOf((*MockWorkerServer)(nil).QueryWorkerConfig), arg0, arg1)
}

// StartSubTask mocks base method
func (m *MockWorkerServer) StartSubTask(arg0 context.Context, arg1 *pb.StartSubTaskRequest) (*pb.CommonWorkerResponse, error) {
	ret := m.ctrl.Call(m, "StartSubTask", arg0, arg1)
	ret0, _ := ret[0].(*pb.CommonWorkerResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// StartSubTask indicates an expected call of StartSubTask
func (mr *MockWorkerServerMockRecorder) StartSubTask(arg0, arg1 interface{}) *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "StartSubTask", reflect.TypeOf((*MockWorkerServer)(nil).StartSubTask), arg0, arg1)
}

// SwitchRelayMaster mocks base method
func (m *MockWorkerServer) SwitchRelayMaster(arg0 context.Context, arg1 *pb.SwitchRelayMasterRequest) (*pb.CommonWorkerResponse, error) {
	ret := m.ctrl.Call(m, "SwitchRelayMaster", arg0, arg1)
	ret0, _ := ret[0].(*pb.CommonWorkerResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// SwitchRelayMaster indicates an expected call of SwitchRelayMaster
func (mr *MockWorkerServerMockRecorder) SwitchRelayMaster(arg0, arg1 interface{}) *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SwitchRelayMaster", reflect.TypeOf((*MockWorkerServer)(nil).SwitchRelayMaster), arg0, arg1)
}

// UpdateRelayConfig mocks base method
func (m *MockWorkerServer) UpdateRelayConfig(arg0 context.Context, arg1 *pb.UpdateRelayRequest) (*pb.CommonWorkerResponse, error) {
	ret := m.ctrl.Call(m, "UpdateRelayConfig", arg0, arg1)
	ret0, _ := ret[0].(*pb.CommonWorkerResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// UpdateRelayConfig indicates an expected call of UpdateRelayConfig
func (mr *MockWorkerServerMockRecorder) UpdateRelayConfig(arg0, arg1 interface{}) *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "UpdateRelayConfig", reflect.TypeOf((*MockWorkerServer)(nil).UpdateRelayConfig), arg0, arg1)
}

// UpdateSubTask mocks base method
func (m *MockWorkerServer) UpdateSubTask(arg0 context.Context, arg1 *pb.UpdateSubTaskRequest) (*pb.CommonWorkerResponse, error) {
	ret := m.ctrl.Call(m, "UpdateSubTask", arg0, arg1)
	ret0, _ := ret[0].(*pb.CommonWorkerResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// UpdateSubTask indicates an expected call of UpdateSubTask
func (mr *MockWorkerServerMockRecorder) UpdateSubTask(arg0, arg1 interface{}) *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "UpdateSubTask", reflect.TypeOf((*MockWorkerServer)(nil).UpdateSubTask), arg0, arg1)
}

// MockWorker_FetchDDLInfoServer is a mock of Worker_FetchDDLInfoServer interface
type MockWorker_FetchDDLInfoServer struct {
	ctrl     *gomock.Controller
	recorder *MockWorker_FetchDDLInfoServerMockRecorder
}

// MockWorker_FetchDDLInfoServerMockRecorder is the mock recorder for MockWorker_FetchDDLInfoServer
type MockWorker_FetchDDLInfoServerMockRecorder struct {
	mock *MockWorker_FetchDDLInfoServer
}

// NewMockWorker_FetchDDLInfoServer creates a new mock instance
func NewMockWorker_FetchDDLInfoServer(ctrl *gomock.Controller) *MockWorker_FetchDDLInfoServer {
	mock := &MockWorker_FetchDDLInfoServer{ctrl: ctrl}
	mock.recorder = &MockWorker_FetchDDLInfoServerMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use
func (m *MockWorker_FetchDDLInfoServer) EXPECT() *MockWorker_FetchDDLInfoServerMockRecorder {
	return m.recorder
}

// Context mocks base method
func (m *MockWorker_FetchDDLInfoServer) Context() context.Context {
	ret := m.ctrl.Call(m, "Context")
	ret0, _ := ret[0].(context.Context)
	return ret0
}

// Context indicates an expected call of Context
func (mr *MockWorker_FetchDDLInfoServerMockRecorder) Context() *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Context", reflect.TypeOf((*MockWorker_FetchDDLInfoServer)(nil).Context))
}

// Recv mocks base method
func (m *MockWorker_FetchDDLInfoServer) Recv() (*pb.DDLLockInfo, error) {
	ret := m.ctrl.Call(m, "Recv")
	ret0, _ := ret[0].(*pb.DDLLockInfo)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Recv indicates an expected call of Recv
func (mr *MockWorker_FetchDDLInfoServerMockRecorder) Recv() *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Recv", reflect.TypeOf((*MockWorker_FetchDDLInfoServer)(nil).Recv))
}

// RecvMsg mocks base method
func (m *MockWorker_FetchDDLInfoServer) RecvMsg(arg0 interface{}) error {
	ret := m.ctrl.Call(m, "RecvMsg", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// RecvMsg indicates an expected call of RecvMsg
func (mr *MockWorker_FetchDDLInfoServerMockRecorder) RecvMsg(arg0 interface{}) *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RecvMsg", reflect.TypeOf((*MockWorker_FetchDDLInfoServer)(nil).RecvMsg), arg0)
}

// Send mocks base method
func (m *MockWorker_FetchDDLInfoServer) Send(arg0 *pb.DDLInfo) error {
	ret := m.ctrl.Call(m, "Send", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// Send indicates an expected call of Send
func (mr *MockWorker_FetchDDLInfoServerMockRecorder) Send(arg0 interface{}) *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Send", reflect.TypeOf((*MockWorker_FetchDDLInfoServer)(nil).Send), arg0)
}

// SendHeader mocks base method
func (m *MockWorker_FetchDDLInfoServer) SendHeader(arg0 metadata.MD) error {
	ret := m.ctrl.Call(m, "SendHeader", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// SendHeader indicates an expected call of SendHeader
func (mr *MockWorker_FetchDDLInfoServerMockRecorder) SendHeader(arg0 interface{}) *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SendHeader", reflect.TypeOf((*MockWorker_FetchDDLInfoServer)(nil).SendHeader), arg0)
}

// SendMsg mocks base method
func (m *MockWorker_FetchDDLInfoServer) SendMsg(arg0 interface{}) error {
	ret := m.ctrl.Call(m, "SendMsg", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// SendMsg indicates an expected call of SendMsg
func (mr *MockWorker_FetchDDLInfoServerMockRecorder) SendMsg(arg0 interface{}) *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SendMsg", reflect.TypeOf((*MockWorker_FetchDDLInfoServer)(nil).SendMsg), arg0)
}

// SetHeader mocks base method
func (m *MockWorker_FetchDDLInfoServer) SetHeader(arg0 metadata.MD) error {
	ret := m.ctrl.Call(m, "SetHeader", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// SetHeader indicates an expected call of SetHeader
func (mr *MockWorker_FetchDDLInfoServerMockRecorder) SetHeader(arg0 interface{}) *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SetHeader", reflect.TypeOf((*MockWorker_FetchDDLInfoServer)(nil).SetHeader), arg0)
}

// SetTrailer mocks base method
func (m *MockWorker_FetchDDLInfoServer) SetTrailer(arg0 metadata.MD) {
	m.ctrl.Call(m, "SetTrailer", arg0)
}

// SetTrailer indicates an expected call of SetTrailer
func (mr *MockWorker_FetchDDLInfoServerMockRecorder) SetTrailer(arg0 interface{}) *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SetTrailer", reflect.TypeOf((*MockWorker_FetchDDLInfoServer)(nil).SetTrailer), arg0)
}
