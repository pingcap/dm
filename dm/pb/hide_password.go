package pb

import (
	"go.uber.org/zap/zapcore"
)

var (
	// avoid cycle import, this function should be overwrite by utils.HidePassword
	HidePwdFunc = func(s string) string {
		return s
	}
)

func (m *StartTaskRequest) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	enc.AddString("HidePasswordObject", HidePwdFunc(m.String()))
	return nil
}

func (m *OperateSourceRequest) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	enc.AddString("HidePasswordObject", HidePwdFunc(m.String()))
	return nil
}
