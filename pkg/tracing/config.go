// Copyright 2019 PingCAP, Inc.
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

package tracing

// Config is the configuration for tracer
type Config struct {
	Enable     bool   `yaml:"enable" toml:"enable" json:"enable"`                // whether to enable tracing
	Source     string `yaml:"source" toml:"source" json:"source"`                // trace event source id
	TracerAddr string `yaml:"tracer-addr" toml:"tracer-addr" json:"tracer-addr"` // tracing service rpc address
	BatchSize  int    `yaml:"batch-size" toml:"batch-size" json:"batch-size"`    // upload trace event batch size
	Checksum   bool   `yaml:"checksum" toml:"checksum" json:"checksum"`          // whether to caclculate checksum of data
}
