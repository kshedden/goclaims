package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path"
)

type Config struct {

	// Directory prefix for all SAS files to process
	SourceDir string

	// SAS file names to process
	SASFiles []string

	// Results are written here
	TargetDir string

	// Read this number of rows at a time from the SAS files
	SASChunkSize uint64

	// Split the data into this number of buckers
	NumBuckets uint32

	// Store this number of buckets in memory before writing to disk
	BufMaxRecs uint64
}

var (
	// Size in bytes of each data type.
	DTsize = map[string]int{"uint8": 1, "uint16": 2, "uint32": 4, "uint64": 8}
)

func ReadConfig(filename string) *Config {

	config := new(Config)

	fid, err := os.Open(filename)
	if err != nil {
		panic(err)
	}
	defer fid.Close()
	dec := json.NewDecoder(fid)
	err = dec.Decode(&config)
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

// ReadDtypes returns the dtypes map for a given bucket.  The dtypes
// map is a map from variable names to their data type.
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

func ReadFactorCodes(varname string, conf *Config) map[string]int {

	fn := path.Join(conf.TargetDir, varname+".json")
	fid, err := os.Open(fn)
	if err != nil {
		panic(err)
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

func RevCodes(codes map[string]int) map[int]string {

	rcodes := make(map[int]string)

	for k, v := range codes {
		rcodes[v] = k
	}

	return rcodes
}
