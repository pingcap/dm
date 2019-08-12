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
	f, err := os.Open(templateCheckerFile)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	content, err := ioutil.ReadAll(f)
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
