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

package encrypt

import (
	"crypto/rand"
	"testing"

	. "github.com/pingcap/check"
)

var _ = Suite(&testEncryptSuite{})

func TestSuite(t *testing.T) {
	TestingT(t)
}

type testEncryptSuite struct {
}

func (t *testEncryptSuite) TestSetSecretKey(c *C) {
	// 16 bit
	b16 := make([]byte, 16)
	_, err := rand.Read(b16)
	c.Assert(err, IsNil)

	err = SetSecretKey(b16)
	c.Assert(err, IsNil)

	// 20 bit
	b20 := make([]byte, 20)
	_, err = rand.Read(b20)
	c.Assert(err, IsNil)

	err = SetSecretKey(b20)
	c.Assert(err, NotNil)
}

func removeChar(input []byte, c byte) []byte {
	i := 0
	for _, x := range input {
		if x != c {
			input[i] = x
			i++
		}
	}
	return input[:i]
}

func (t *testEncryptSuite) TestEncrypt(c *C) {
	plaintext := []byte("a plain text")

	// encrypt
	ciphertext, err := Encrypt(plaintext)
	c.Assert(err, IsNil)

	// decrypt
	plaintext2, err := Decrypt(ciphertext)
	c.Assert(err, IsNil)
	c.Assert(plaintext2, DeepEquals, plaintext)

	// invalid length
	_, err = Decrypt(ciphertext[:len(ciphertext)-len(plaintext)-1])
	c.Assert(err, NotNil)

	// invalid content
	_, err = Decrypt(removeChar(ciphertext, ivSep[0]))
	c.Assert(err, NotNil)
}
