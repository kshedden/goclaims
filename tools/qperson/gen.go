// +build ignore

package main

import (
	"bytes"
	"go/format"
	"os"
	"text/template"
)

func main() {

	if len(os.Args) != 2 {
		panic("wrong number of arguments")
	}

	templateName := os.Args[1]

	tmpl, err := template.ParseFiles(templateName)
	if err != nil {
		panic(err)
	}

	type tp struct {
		Name   string
		Format string
	}

	rtypes := []tp{
		tp{"uint8", "%d"},
		tp{"uint16", "%d"},
		tp{"uint32", "%d"},
		tp{"uint64", "%d"},
		tp{"float32", "%f"},
		tp{"float64", "%f"},
	}

	var buf bytes.Buffer
	err = tmpl.Execute(&buf, rtypes)
	if err != nil {
		panic(err)
	}

	p, err := format.Source(buf.Bytes())
	if err != nil {
		panic(err)
	}

	out, err := os.Create("generated_defs.go")
	if err != nil {
		panic(err)
	}
	out.WriteString("// GENERATED CODE, DO NOT EDIT\n")
	_, err = out.Write(p)
	if err != nil {
		panic(err)
	}
	out.Close()
}
