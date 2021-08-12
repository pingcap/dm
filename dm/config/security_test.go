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

package config

import (
	"bytes"
	"os"

	. "github.com/pingcap/check"
)

const (
	testdataPath = "./testdata"

	caFile        = "./testdata/ca.pem"
	caFileContent = `
-----BEGIN CERTIFICATE-----
test no content
-----END CERTIFICATE-----
`
	certFile        = "./testdata/cert.pem"
	certFileContent = `
-----BEGIN CERTIFICATE-----
test no content
-----END CERTIFICATE-----
`
	keyFile        = "./testdata/key.pem"
	keyFileContent = `
-----BEGIN RSA PRIVATE KEY-----
test no content
-----END RSA PRIVATE KEY-----
`
)

func createTestFixture(c *C) {
	c.Assert(os.Mkdir(testdataPath, 0o744), IsNil)

	f, err := os.Create(caFile)
	c.Assert(err, IsNil)
	_, err = f.WriteString(caFileContent)
	c.Assert(err, IsNil)
	f.Close()

	f, err = os.Create(certFile)
	c.Assert(err, IsNil)
	_, err = f.WriteString(certFileContent)
	c.Assert(err, IsNil)
	f.Close()

	f, err = os.Create(keyFile)
	c.Assert(err, IsNil)
	_, err = f.WriteString(keyFileContent)
	c.Assert(err, IsNil)
	f.Close()
}

func clearTestFixture(c *C) {
	c.Assert(os.RemoveAll(testdataPath), IsNil)
}

func (t *testConfig) TestLoadAndClearContent(c *C) {
	createTestFixture(c)
	defer clearTestFixture(c)

	s := &Security{
		SSLCA:   "testdata/ca.pem",
		SSLCert: "testdata/cert.pem",
		SSLKey:  "testdata/key.pem",
	}
	err := s.LoadTLSContent()
	c.Assert(err, IsNil)
	c.Assert(len(s.SSLCABytes) > 0, Equals, true)
	c.Assert(len(s.SSLCertBytes) > 0, Equals, true)
	c.Assert(len(s.SSLKEYBytes) > 0, Equals, true)

	noContentBytes := []byte("test no content")

	c.Assert(bytes.Contains(s.SSLCABytes, noContentBytes), Equals, true)
	c.Assert(bytes.Contains(s.SSLKEYBytes, noContentBytes), Equals, true)
	c.Assert(bytes.Contains(s.SSLCertBytes, noContentBytes), Equals, true)

	s.ClearSSLBytesData()
	c.Assert(s.SSLCABytes, HasLen, 0)
	c.Assert(s.SSLCertBytes, HasLen, 0)
	c.Assert(s.SSLKEYBytes, HasLen, 0)
}
