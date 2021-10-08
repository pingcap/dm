// Copyright 2021 PingCAP, Inc.
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

package openapi

import (
	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/pkg/terror"
)

var defaultMetaSchema = "dm_meta"

// Adjust adjusts task and set default value.
func (t *Task) Adjust() error {
	if t.MetaSchema == nil {
		t.MetaSchema = &defaultMetaSchema
	}
	// check some not implemented features
	if t.OnDuplicate != TaskOnDuplicateError {
		return terror.ErrOpenAPICommonError.Generate("`on_duplicate` only supports `error` for now.")
	}
	return nil
}

// GetTargetDBCfg gets target db config.
func (t *Task) GetTargetDBCfg() *config.DBConfig {
	toDBCfg := &config.DBConfig{
		Host:     t.TargetConfig.Host,
		Port:     t.TargetConfig.Port,
		User:     t.TargetConfig.User,
		Password: t.TargetConfig.Password,
	}
	if t.TargetConfig.Security != nil {
		var certAllowedCn []string
		if t.TargetConfig.Security.CertAllowedCn != nil {
			certAllowedCn = append(certAllowedCn, *t.TargetConfig.Security.CertAllowedCn...)
		}
		toDBCfg.Security = &config.Security{
			SSLCABytes:    []byte(t.TargetConfig.Security.SslCaContent),
			SSLKEYBytes:   []byte(t.TargetConfig.Security.SslKeyContent),
			SSLCertBytes:  []byte(t.TargetConfig.Security.SslCertContent),
			CertAllowedCN: certAllowedCn,
		}
	}
	return toDBCfg
}
