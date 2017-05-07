/*
Package config contains a configuration structure and other utility
routines that are shared among the packages in this project.
*/

package config

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strings"

	"github.com/BurntSushi/toml"
)

type Config struct {

	// Directory prefix for all SAS files to process
	SourceDir string

	// SAS file names to process
	SASFiles []string

	// Results are written here
	TargetDir string

	// The directory where factor codes are stored
	CodesDir string

	// Read this number of rows at a time from the SAS files
	SASChunkSize uint64

	// Split the data into this number of buckets
	NumBuckets uint32

	// Store this number of buckets in memory before writing to disk
	BufMaxRecs uint64

	// Process only this number of chunks.  If zero, all the
	// chunks are processed.
	MaxChunk uint32
}

var (
	// Size in bytes of each data type.
	DTsize = map[string]int{"uint8": 1, "uint16": 2, "uint32": 4, "uint64": 8, "float32": 4, "float64": 8}
)

// ReadConfig returns the configuration information stored at the
// given file path.
func ReadConfig(filename string) *Config {

	config := new(Config)

	fid, err := os.Open(filename)
	if err != nil {
		panic(err)
	}
	defer fid.Close()
	td, err := ioutil.ReadAll()
	if err != nil {
		panic(err)
	}

	_, err = toml.Decode(td, &config)
	if err != nil {
		panic(err)
	}

	return config
}

// BucketPath returns the path to the given bucket.
func BucketPath(bucket int, conf *Config) string {
	b := fmt.Sprintf("%04d", bucket)
	return path.Join(conf.TargetDir, "Buckets", b)
}

// ReadDtypes returns a map describing the column data types map for a
// given bucket.  The dtypes map associates variable names with their
// data type (e.g. uint8).
func ReadDtypes(bucket int, conf *Config) map[string]string {

	dtypes := make(map[string]string)

	p := BucketPath(bucket, conf)
	fn := path.Join(p, "dtypes.json")

	fid, err := os.Open(fn)
	if err != nil {
		panic(err)
	}
	defer fid.Close()
	dec := json.NewDecoder(fid)
	err = dec.Decode(&dtypes)
	if err != nil {
		panic(err)
	}

	return dtypes
}

// ReadFactorCodes returns a map from strings to integers describing a
// factor-coded variable.
func ReadFactorCodes(varname string, conf *Config) map[string]int {

	fn := path.Join(conf.CodesDir, varname+".json")
	fid, err := os.Open(fn)
	if err != nil {
		msg := fmt.Sprintf("Can't open codes file %s\n", fn)
		os.Stderr.WriteString(msg)
		os.Exit(1)
	}
	defer fid.Close()

	dec := json.NewDecoder(fid)
	mp := make(map[string]int)
	err = dec.Decode(&mp)
	if err != nil {
		panic(err)
	}

	return mp
}

// RevFactorCodes returns the reverse factor coding map, associating
// integers with their corresponding string label.
func RevCodes(codes map[string]int) map[int]string {

	rcodes := make(map[int]string)

	for k, v := range codes {
		rcodes[v] = k
	}

	return rcodes
}

// VarDesc is a description of a variable to be ported from a SAS
// file.  Name, GoType, and SASType must be provided, and Must is
// optional.
type VarDesc struct {

	// Name of the variable in the output datasets.
	Name string

	// Data type of the variable in the output datasets.
	Type string

	// Type of the variable in the initial conversion from SAS.
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
func GetVarDefs(filename string) []*VarDesc {

	fid, err := os.Open(filename)
	if err != nil {
		panic(err)
	}
	defer fid.Close()

	var vdesca []*VarDesc
	dec := json.NewDecoder(fid)
	for dec.More() {
		x := new(VarDesc)
		err = dec.Decode(&x)
		if err != nil {
			panic(err)
		}
		vdesca = append(vdesca, x)
	}

	for _, v := range vdesca {
		v.SASName = strings.ToUpper(v.Name)
		v.SASTypeU = strings.Title(v.SASType)

		if v.Type == "" {
			v.Type = v.GoType
		}
	}

	return vdesca
}
