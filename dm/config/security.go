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

package config

import "fmt"

// Security config
type Security struct {
	SSLCA         string   `toml:"ssl-ca" json:"ssl-ca" yaml:"ssl-ca"`
	SSLCert       string   `toml:"ssl-cert" json:"ssl-cert" yaml:"ssl-cert"`
	SSLKey        string   `toml:"ssl-key" json:"ssl-key" yaml:"ssl-key"`
	CertAllowedCN strArray `toml:"cert-allowed-cn" json:"cert-allowed-cn" yaml:"cert-allowed-cn"`
}

// used for parse string slice in flag
type strArray []string

func (i *strArray) String() string {
	return fmt.Sprint([]string(*i))
}

func (i *strArray) Set(value string) error {
	*i = append(*i, value)
	return nil
}
