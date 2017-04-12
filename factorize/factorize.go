/*
Factorize converts strings to uint16 codes, and saves the mapping from strings to codes as a json file.
*/

package factorize

import (
	"bufio"
	"compress/gzip"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"

	"github.com/kshedden/goclaims/config"
)

var (
	codes   map[string]int
	changes map[int][]string

	conf *config.Config

	logger *log.Logger

	mut sync.Mutex
	wg  sync.WaitGroup
)

func dofile(file string) {

	logger.Printf("Processing %s", file)

	// Track changes to dtypes
	pa := path.Dir(file)
	pa = path.Base(pa)
	bn, err := strconv.Atoi(pa)
	if err != nil {
		panic(err)
	}
	vn := path.Base(file)
	vn = strings.Split(vn, ".")[0]
	changes[bn] = append(changes[bn], vn)

	origfile := strings.Replace(file, ".bin.gz", "_orig.bin.gz", 1)
	err = os.Rename(file, origfile)

	fid, err := os.Open(origfile)
	if err != nil {
		panic(err)
	}
	defer fid.Close()
	rdr, err := gzip.NewReader(fid)
	if err != nil {
		msg := fmt.Sprintf("Corrupt gzip file: %s\n", file)
		os.Stderr.WriteString(msg)
		panic(err)
	}
	defer rdr.Close()

	out, err := os.Create(file)
	if err != nil {
		panic(err)
	}
	defer out.Close()
	wtr := gzip.NewWriter(out)
	defer wtr.Close()

	scanner := bufio.NewScanner(rdr)
	for scanner.Scan() {
		tok := scanner.Text()

		mut.Lock()
		c, ok := codes[tok]
		if !ok {
			c = len(codes)
			codes[tok] = c
		}
		mut.Unlock()

		err := binary.Write(wtr, binary.LittleEndian, uint16(c))
		if err != nil {
			panic(err)
		}
	}

	err = os.Remove(origfile)
	if err != nil {
		panic(err)
	}

	wg.Done()
}

func updateDtypes() {

	dtypes := make(map[string]string)

	logger.Printf("Changing dtypes: %v", changes)

	for k := 0; k < int(conf.NumBuckets); k++ {

		vn, ok := changes[k]
		if !ok {
			continue
		}

		fn := path.Join(conf.TargetDir, "Buckets", fmt.Sprintf("%04d", k), "dtypes.json")
		fid, err := os.Open(fn)
		dec := json.NewDecoder(fid)
		err = dec.Decode(&dtypes)
		if err != nil {
			panic(err)
		}
		fid.Close()

		for _, v := range vn {
			dtypes[v] = "uint16"
		}

		fid, err = os.Create(fn)
		enc := json.NewEncoder(fid)
		err = enc.Encode(dtypes)
		if err != nil {
			panic(err)
		}
		fid.Close()
	}
}

func Run(cnf *config.Config, files []string, jsonname string, lgr *log.Logger) {

	logger = lgr
	conf = cnf

	codes = make(map[string]int)
	codes[""] = 0 // make sure the zero-value refers to null code

	changes = make(map[int][]string)

	for _, file := range files {
		wg.Add(1)
		fn := path.Join(conf.TargetDir, file)
		dofile(fn)
	}

	wg.Wait()

	updateDtypes()

	fn := path.Join(conf.TargetDir, jsonname)
	fid, err := os.Create(fn)
	if err != nil {
		panic(err)
	}
	defer fid.Close()
	enc := json.NewEncoder(fid)
	err = enc.Encode(codes)
	if err != nil {
		panic(err)
	}
}
