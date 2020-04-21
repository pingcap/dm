// Copyright 2020 PingCAP, Inc.
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

package plugin

import (
	"plugin"

	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/pkg/terror"
	"github.com/siddontang/go-mysql/replication"
)

var (
	createPluginFunc = "NewPlugin"
)

// LoadPlugin loads plugin by plugin's file path
func LoadPlugin(filepath string) (Plugin, error) {
	if len(filepath) == 0 {
		return new(NilPlugin), nil
	}

	p, err := plugin.Open(filepath)
	if err != nil {
		return nil, terror.ErrSyncerLoadPlugin.Delegate(err, filepath)
	}

	pluginSymbol, err := p.Lookup(createPluginFunc)
	if err != nil {
		return nil, terror.ErrSyncerLoadPlugin.Delegate(err, filepath)
	}

	newPlugin, ok := pluginSymbol.(func() interface{})
	if !ok {
		return nil, terror.ErrSyncerLoadPlugin.Delegate(err, filepath)
	}

	plg := newPlugin()
	plg2, ok := plg.(Plugin)
	if !ok {
		return nil, terror.ErrSyncerLoadPlugin.Delegate(err, filepath)
	}

	return plg2, nil
}

// Plugin is a struct of plugin used in syncer unit
type Plugin interface {
	// Init do some init job
	Init(cfg *config.SubTaskConfig) error

	// HandleDDLJobResult handles the result of ddl job
	HandleDDLJobResult(ev *replication.QueryEvent, err error) error

	// HandleDMLJobResult handles the result of dml job
	HandleDMLJobResult(ev *replication.RowsEvent, err error) error
}

// NilPlugin is a plugin which do nothing
type NilPlugin struct{}

// Init implements Plugin's Init
func (n *NilPlugin) Init(cfg *config.SubTaskConfig) error {
	return nil
}

// HandleDDLJobResult implements Plugin's HandleDDLJobResult
func (n *NilPlugin) HandleDDLJobResult(ev *replication.QueryEvent, err error) error {
	return err
}

// HandleDMLJobResult implements Plugin's HandleDMLJobResult
func (n *NilPlugin) HandleDMLJobResult(ev *replication.RowsEvent, err error) error {
	return err
}
