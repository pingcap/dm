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

package utils

import (
	"fmt"
	"strings"

	"github.com/pingcap/dm/dm/pb"
)

const (
	defaultStringLenLimit = 1024
)

// TruncateString returns a string with only the leading (at most) n runes of the input string.
// If the string is truncated, a `...` tail will be appended.
func TruncateString(s string, n int) string {
	if n < 0 {
		n = defaultStringLenLimit
	}
	if len(s) <= n {
		return s
	}
	return s[:n] + "..." // mark as some content is truncated
}

// TruncateInterface converts the interface to a string
// and returns a string with only the leading (at most) n runes of the input string.
// If the converted string is truncated, a `...` tail will be appended.
// It is not effective for large structure now.
func TruncateInterface(v interface{}, n int) string {
	// because []byte will print ascii using Printf("%+v"), we use "%s" for this type
	prettyPrint0 := func(v interface{}) string {
		if vv, ok := v.([]byte); ok {
			return fmt.Sprintf("%s", vv)
		}
		return fmt.Sprintf("%+v", v)
	}
	prettyPrint1 := func(v interface{}) string {
		if _, ok := v.([]byte); ok {
			return prettyPrint0(v)
		}

		if vv, ok := v.([]interface{}); ok {
			var buf strings.Builder
			buf.WriteString("[")
			for i, v2 := range vv {
				if i != 0 {
					buf.WriteString(" ")
				}
				buf.WriteString(prettyPrint0(v2))
			}
			buf.WriteString("]")
			return buf.String()
		}
		return fmt.Sprintf("%+v", v)
	}
	prettyPrint2 := func(v interface{}) string {
		if _, ok := v.([]byte); ok {
			return prettyPrint0(v)
		}
		if _, ok := v.([]interface{}); ok {
			return prettyPrint1(v)
		}

		if vv, ok := v.([][]interface{}); ok {
			var buf strings.Builder
			buf.WriteString("[")
			for i, v2 := range vv {
				if i != 0 {
					buf.WriteString(" ")
				}
				buf.WriteString(prettyPrint1(v2))
			}
			buf.WriteString("]")
			return buf.String()
		}
		return fmt.Sprintf("%+v", v)
	}

	return TruncateString(prettyPrint2(v), n)
}

// JoinProcessErrors return the string of pb.ProcessErrors joined by ", "
func JoinProcessErrors(errors []*pb.ProcessError) string {
	serrs := make([]string, 0, len(errors))
	for _, serr := range errors {
		serrs = append(serrs, serr.String())
	}
	return strings.Join(serrs, ", ")
}
