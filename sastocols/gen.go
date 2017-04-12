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

type tvals struct {
	Dtypes   string
	NameType []*vardesc
}

type vardesc struct {
	Name     string
	GoType   string
	SASType  string
	SASName  string
	SASTypeU string
	Must     bool
}

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

	tmpl, err := template.ParseFiles("defs.template")
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
