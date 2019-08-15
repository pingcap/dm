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

package main

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/ioutil"
	"os"
	"strings"
)

const (
	templateCheckerFile  = "checker_template.go"
	generatedCheckerFile = "checker_generated.go"
)

var templatePlaceHolder = []byte("// TODO: fillin me")

func getErrorInstances(filepath string) []string {
	fset := token.NewFileSet()
	parserMode := parser.ParseComments
	var (
		fileAst *ast.File
		err     error
		result  = make([]string, 0, 100)
	)

	fileAst, err = parser.ParseFile(fset, filepath, nil, parserMode)
	if err != nil {
		panic(err)
	}

	for _, d := range fileAst.Decls {
		switch decl := d.(type) {
		case *ast.GenDecl:
			specs := decl.Specs
			for _, spec := range specs {
				vs, ok := spec.(*ast.ValueSpec)
				if !ok {
					continue
				}
				name := vs.Names[0].Name
				if strings.HasPrefix(name, "Err") {
					result = append(result, name)
				}
			}
		}
	}
	return result
}

func genFile(names []string) {
	content, err := ioutil.ReadFile(templateCheckerFile)
	if err != nil {
		panic(err)
	}
	var buf bytes.Buffer
	for i, name := range names {
		tab := "\t"
		if i == 0 {
			tab = ""
		}
		fmt.Fprintf(&buf, "%s{\"%s\", terror.%s},\n", tab, name, name)
	}
	newContent := bytes.ReplaceAll(content, templatePlaceHolder, buf.Bytes())
	err = ioutil.WriteFile(generatedCheckerFile, newContent, 0644)
	if err != nil {
		panic(err)
	}
}

func main() {
	names := getErrorInstances(os.Args[1])
	genFile(names)
}
