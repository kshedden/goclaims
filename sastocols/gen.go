// Code generation script for sastocols.  Takes a variable definition
// file in json format and generates code for bucketing and
// columnizing a SAS file containing such variables.  This script is
// run automatically via the command:
//
// go generate procsas.go

// +build ignore

package main

import (
	"bytes"
	"encoding/json"
	"go/format"
	"os"
	"path"
	"text/template"

	"github.com/kshedden/goclaims/config"
)

const (
	templateName = "defs.template"
)

// tvals contains values that are to be insterted into the code
// template.
type tvals struct {
	Ftype    string
	Rtypes   []string
	Dtypes   string
	NameType []*config.VarDesc
}

// getdtypes returns a json encoded map describing the dtypes, based
// on the array of variable descriptions.
func getdtypes(nametype []*config.VarDesc) string {

	mp := make(map[string]string)

	for _, v := range nametype {
		mp[v.Name] = v.GoType
	}

	var bbuf bytes.Buffer
	enc := json.NewEncoder(&bbuf)
	err := enc.Encode(mp)
	if err != nil {
		panic(err)
	}

	return string(bbuf.Bytes())
}

func main() {

	if len(os.Args) != 3 {
		panic("wrong number of arguments")
	}

	ftype := os.Args[1]
	vdesca := config.GetVarDefs(os.Args[2])

	tmpl, err := template.ParseFiles(templateName)
	if err != nil {
		panic(err)
	}

	rtypes := []string{"uint8", "uint16", "uint32", "uint64", "float32", "float64"}

	tval := &tvals{
		Ftype:    ftype,
		Rtypes:   rtypes,
		NameType: vdesca,
		Dtypes:   getdtypes(vdesca),
	}

	var buf bytes.Buffer
	err = tmpl.Execute(&buf, tval)
	if err != nil {
		panic(err)
	}

	p, err := format.Source(buf.Bytes())
	if err != nil {
		panic(err)
	}

	fp := path.Join(ftype+"files", "generated_defs.go")
	out, err := os.Create(fp)
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
