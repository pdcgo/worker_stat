package main

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"log"
	"log/slog"
	"os"
	"reflect"
	"strconv"
	"strings"
)

func main() {
	fname := os.Getenv("GOFILE")
	lineStr := os.Getenv("GOLINE")
	line, _ := strconv.Atoi(lineStr)

	if fname == "" {
		fname = "example/model.go"
		line = 4
	}

	outfname := strings.ReplaceAll(fname, ".go", "_stream_gen.go")

	slog.Info("generating", "fname", fname)

	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, fname, nil, parser.ParseComments)
	if err != nil {
		log.Fatal(err)
	}

	var buf bytes.Buffer
	buf.WriteString(
		fmt.Sprintf("package %s", file.Name.Name),
	)

	var structd *ast.TypeSpec

	ast.Inspect(file, func(n ast.Node) bool {
		if n == nil {
			return true
		}

		dd, ok := n.(*ast.TypeSpec)
		if !ok {
			return true
		}

		pos := fset.Position(dd.Pos())
		if pos.Line == line {
			structd = dd
		}

		return true
	})

	if structd == nil {
		return
	}

	log.Println("asdasd", structd.Name)

	// writing to destination
	out, err := format.Source(buf.Bytes())
	if err != nil {
		panic(err)
	}

	os.WriteFile(outfname, out, 0644)
}

func debugName(d any) {
	log.Println(reflect.TypeOf(d).Name())
}
