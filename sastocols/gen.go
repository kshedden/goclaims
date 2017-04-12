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
	"strings"
	"text/template"
)

const (
	templateName = "defs.template"
)

// tvals contains values that are to be insterted into the code
// template.
type tvals struct {
	Dtypes   string
	NameType []*vardesc
}

// vardesc is a description of a variable to be handled by this
// script.  Name, GoType, and SASType must be provided, and Must is
// optional.  SASName
type vardesc struct {

	// Name of the variable in the output datasets.
	Name string

	// Data type of the variable in the output datasets.
	GoType string

	// Type of the variable in the SAS datasets, must be float64
	// or string.
	SASType string

	// If true, produces an error if the variable is not present.
	// Otherwise silently skips processing this variable when it
	// is not present.
	Must bool

	SASName  string // used internally
	SASTypeU string // used internally
}

// Read a json file containing the variable information.
func getTvals() *tvals {

	fid, err := os.Open(os.Args[1])
	if err != nil {
		panic(err)
	}
	defer fid.Close()

	var vdesca []*vardesc
	dec := json.NewDecoder(fid)
	for dec.More() {
		x := new(vardesc)
		err = dec.Decode(&x)
		if err != nil {
			panic(err)
		}
		vdesca = append(vdesca, x)
	}

	for _, v := range vdesca {
		v.SASName = strings.ToUpper(v.Name)
		v.SASTypeU = strings.Title(v.SASType)
	}

	return &tvals{
		NameType: vdesca,
		Dtypes:   getdtypes(vdesca),
	}
}

// getdtypes returns a json encoded map describing the dtypes, based
// on the array of variable descriptions.
func getdtypes(nametype []*vardesc) string {

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

	tval := getTvals()

	tmpl, err := template.ParseFiles(templateName)
	if err != nil {
		panic(err)
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
