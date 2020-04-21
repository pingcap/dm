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
	//"fmt"

	//"github.com/pingcap/errors"
	"github.com/pingcap/dm/dm/config"
	"github.com/siddontang/go-mysql/replication"
)

var (
	createPluginFunc = "NewPlugin"
)

// LoadPlugin loads plugin by plugin's file path
func LoadPlugin(filepath string) (Plugin, error) {
	p, err := plugin.Open(filepath)
	if err != nil {
		// TODO: use terror
		return nil, err
	}

	pluginSymbol, err := p.Lookup(createPluginFunc)
	if err != nil {
		return nil, err
	}

	newPlugin, ok := pluginSymbol.(func() interface{})
	if !ok {
		// TODO: use terror
		return nil, nil
	}

	plg := newPlugin()
	plg2, ok := plg.(Plugin)
	if !ok {
		return nil, nil
	}

	return plg2, nil
	/*
		syncerPlugin.Init = initFunc


		plg := newPlugin()
		handleDDLJobResultFunc, ok := plg.(HandleDDLJobResult)
		if !ok {
			return nil, nil
		}
		syncerPlugin.HandleDDLJobResult = handleDDLJobResultFunc


		plg := newPlugin()
		HandleDMLJobResultFunc, ok := plg.(HandleDMLJobResult)
		if !ok {
			return nil, nil
		}
		syncerPlugin.HandleDMLJobResult = HandleDMLJobResultFunc

		return syncerPlugin
	*/
}

// Plugin is a struct of plugin used in syncer unit
type Plugin interface {
	// the plugin's .so file path
	//path string

	// conn used to creating connection for double write or other usage
	//conn interface{}

	// Init do some init job
	Init(cfg *config.SubTaskConfig) error

	// HandleDDLJobResult handles the result of ddl job
	HandleDDLJobResult(ev *replication.QueryEvent, err error) error

	// HandleDMLJobResult handles the result of dml job
	HandleDMLJobResult(ev *replication.RowsEvent, err error) error
}
