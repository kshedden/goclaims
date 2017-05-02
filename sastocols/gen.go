// Code generation script for sastocols.  Takes a variable definition
// file in json format and generates code for bucketing and
// columnizing a set of SAS files, each of which contains the
// variables as described in the variable definition file.
//
// To generate a go script, run this script as follows:
//
// go run gen.go vdefs.json > sastocols.go
//
// The resulting sastocols.go script is a Go script that can be run
// as follows:
//
// go run sastocols.go config.json
//
// where config.json is a configuration script as in
// github.com/kshedden/gosascols/config

package main

import (
	"bytes"
	"encoding/json"
	"go/format"
	"os"
	"text/template"

	"github.com/kshedden/gosascols/config"
)

// tvals contains values that are to be insterted into the code
// template.
type tvals struct {
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

	if len(os.Args) != 2 {
		panic("wrong number of arguments")
	}

	vdesca := config.GetVarDefs(os.Args[1])

	tmpl, err := template.ParseFiles("templ.got")
	if err != nil {
		panic(err)
	}

	rtypes := []string{"uint8", "uint16", "uint32", "uint64", "float32", "float64"}

	tval := &tvals{
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

	os.Stdout.WriteString("// GENERATED CODE, DO NOT EDIT\n")
	_, err = os.Stdout.Write(p)
	if err != nil {
		panic(err)
	}
}
