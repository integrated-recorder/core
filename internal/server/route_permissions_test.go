package server

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
)

// Every product API registered on the Control Plane mux must either be an
// explicitly public authentication bootstrap/session route or have an exact
// entry in the fail-closed permission table.
func TestEveryRegisteredAPIHasAuthorizationClassification(t *testing.T) {
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate server package source")
	}
	root := filepath.Dir(source)
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	files := token.NewFileSet()
	registered := map[string]struct{}{}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") || strings.HasSuffix(entry.Name(), "_test.go") {
			continue
		}
		file, err := parser.ParseFile(files, filepath.Join(root, entry.Name()), nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", entry.Name(), err)
		}
		ast.Inspect(file, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok || len(call.Args) == 0 {
				return true
			}
			selector, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || (selector.Sel.Name != "HandleFunc" && selector.Sel.Name != "Handle") {
				return true
			}
			pattern, ok := call.Args[0].(*ast.BasicLit)
			if !ok || pattern.Kind != token.STRING {
				return true
			}
			value, err := strconv.Unquote(pattern.Value)
			if err != nil || !strings.Contains(value, "/api/") {
				return true
			}
			fields := strings.Fields(value)
			if len(fields) == 1 {
				return true // API subtree mounts are classified by their owning handler.
			}
			if len(fields) != 2 || !strings.HasPrefix(fields[1], "/api/") {
				return true
			}
			key := fields[0] + " " + fields[1]
			registered[key] = struct{}{}
			return true
		})
	}
	public := map[string]bool{
		"GET /api/auth/session":    true,
		"POST /api/auth/login":     true,
		"POST /api/auth/bootstrap": true,
	}
	for route := range registered {
		if public[route] {
			continue
		}
		parts := strings.SplitN(route, " ", 2)
		if len(parts) != 2 || routePermission(parts[0], parts[1]) == "permission.unclassified" {
			t.Errorf("registered API %q has no explicit authorization classification", route)
		}
	}
	if len(registered) == 0 {
		t.Fatal("no API route registrations were inspected")
	}
}
