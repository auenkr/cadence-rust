// Audit the original Go source with its AST. This is migration/compatibility
// tooling; the Rust server neither imports it nor needs a Go compiler.
package main

import (
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

type Symbol struct {
	Kind string `json:"kind"`
	Name string `json:"name"`
	Line int    `json:"line"`
}
type Source struct {
	Path    string   `json:"goPath"`
	Package string   `json:"package"`
	Kind    string   `json:"kind"`
	Symbols []Symbol `json:"symbols"`
}

func main() {
	root := "go"
	if len(os.Args) > 1 {
		root = os.Args[1]
	}
	var sources []Source
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			if entry.Name() == ".git" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") {
			return nil
		}
		set := token.NewFileSet()
		file, err := parser.ParseFile(set, path, nil, parser.ParseComments|parser.AllErrors)
		if err != nil {
			return fmt.Errorf("parse %s: %w", path, err)
		}
		kind := "production"
		if strings.HasSuffix(path, "_test.go") {
			kind = "test"
		} else if ast.IsGenerated(file) {
			kind = "generated"
		}
		source := Source{Path: filepath.ToSlash(path), Package: file.Name.Name, Kind: kind, Symbols: []Symbol{}}
		for _, decl := range file.Decls {
			switch decl := decl.(type) {
			case *ast.FuncDecl:
				name := decl.Name.Name
				if decl.Recv != nil {
					name = receiverName(decl.Recv.List[0].Type) + "." + name
				}
				source.Symbols = append(source.Symbols, Symbol{Kind: "function", Name: name, Line: set.Position(decl.Pos()).Line})
			case *ast.GenDecl:
				for _, spec := range decl.Specs {
					switch spec := spec.(type) {
					case *ast.TypeSpec:
						source.Symbols = append(source.Symbols, Symbol{Kind: "type", Name: spec.Name.Name, Line: set.Position(spec.Pos()).Line})
					case *ast.ValueSpec:
						for _, name := range spec.Names {
							source.Symbols = append(source.Symbols, Symbol{Kind: strings.ToLower(decl.Tok.String()), Name: name.Name, Line: set.Position(name.Pos()).Line})
						}
					}
				}
			}
		}
		sources = append(sources, source)
		return nil
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err = json.NewEncoder(os.Stdout).Encode(sources); err != nil {
		panic(err)
	}
}
func receiverName(expr ast.Expr) string {
	switch expr := expr.(type) {
	case *ast.Ident:
		return expr.Name
	case *ast.StarExpr:
		return "*" + receiverName(expr.X)
	case *ast.IndexExpr:
		return receiverName(expr.X)
	case *ast.IndexListExpr:
		return receiverName(expr.X)
	}
	return "receiver"
}
