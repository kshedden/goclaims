/*
Package factorize converts strings to uint32 codes, and saves the
mapping from strings to codes as a json file.
*/

package factorize

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path"
	"sort"
	"strings"

	"github.com/golang/snappy"
)

const (
	concurrency = 20
)

type xfunc func(string) string

var (
	freq map[string]uint64

	codes map[string]int

	sem chan bool

	logger *log.Logger

	codesFile string

	xf xfunc
)

func dofile(file string) {

	defer func() { <-sem }()

	logger.Printf("Processing %s", file)

	origfile := strings.Replace(file, ".bin.sz", "_string.bin.sz", 1)
	err := os.Rename(file, origfile)
	if err != nil {
		panic(err)
	}

	// Origin
	fid, err := os.Open(origfile)
	if err != nil {
		panic(err)
	}
	defer fid.Close()
	rdr := snappy.NewReader(fid)

	// Destination
	out, err := os.Create(file)
	if err != nil {
		panic(err)
	}
	defer out.Close()
	wtr := snappy.NewBufferedWriter(out)
	defer wtr.Close()

	buf := make([]byte, 8)

	scanner := bufio.NewScanner(rdr)
	var jj int
	for ; scanner.Scan(); jj++ {
		tok := scanner.Text()

		if xf != nil {
			tok = xf(tok)
		}

		c, ok := codes[tok]
		if !ok {
			panic("code not found")
		}

		m := binary.PutUvarint(buf, uint64(c))

		_, err := wtr.Write(buf[0:m])
		if err != nil {
			panic(err)
		}
	}

	if err := scanner.Err(); err != nil {
		panic(err)
	}

	logger.Printf("File: %s\nLen: %d\n", file, jj)
}

func updateDtypes(files []string) {

	logger.Printf("Changing dtypes...")

	// Group the files by directory
	fb := make(map[string][]string)
	for _, f := range files {
		d, g := path.Split(f)
		fb[d] = append(fb[d], g)
	}

	for dir, fl := range fb {

		fn := path.Join(dir, "dtypes.json")
		nn := path.Join(dir, "dtypes_string.json")
		err := os.Rename(fn, nn)
		if err != nil {
			panic(err)
		}

		// Read the current dtypes
		fid, err := os.Open(nn)
		if err != nil {
			panic(err)
		}
		dec := json.NewDecoder(fid)
		dtypes := make(map[string]string)
		err = dec.Decode(&dtypes)
		if err != nil {
			panic(err)
		}
		fid.Close()

		// Modify the dtypes
		for _, f := range fl {
			v := strings.Split(f, ".")[0]
			dtypes[v] = "uvarint"
		}

		// Write the modified types back to disk
		fid, err = os.Create(fn)
		enc := json.NewEncoder(fid)
		err = enc.Encode(dtypes)
		if err != nil {
			panic(err)
		}
		fid.Close()
	}
}

func getfreqfile(file string, sem chan bool, rslt chan map[string]uint64) {

	defer func() { <-sem }()

	fid, err := os.Open(file)
	if err != nil {
		panic(err)
	}
	defer fid.Close()
	rdr := snappy.NewReader(fid)

	cnt := make(map[string]uint64)

	scanner := bufio.NewScanner(rdr)
	for scanner.Scan() {
		tok := scanner.Text()

		if xf != nil {
			tok = xf(tok)
		}

		c, ok := cnt[tok]
		if !ok {
			cnt[tok] = 1
		} else {
			cnt[tok] = c + 1
		}
	}

	if err := scanner.Err(); err != nil {
		panic(err)
	}

	rslt <- cnt
}

func getfreq(files []string) {

	sem := make(chan bool, concurrency)
	rslt := make(chan map[string]uint64, 10)
	freq = make(map[string]uint64)
	hsem := make(chan bool, 1)

	hsem <- true
	go func() {
		for r := range rslt {
			for k, v := range r {
				c, ok := freq[k]
				if !ok {
					freq[k] = v
				} else {
					freq[k] = c + v
				}
			}
		}
		<-hsem
	}()

	for _, file := range files {
		sem <- true
		go getfreqfile(file, sem, rslt)
	}

	for k := 0; k < concurrency; k++ {
		sem <- true
	}
	close(rslt)

	logger.Printf("Done calculating frequencies of %d codes\n", len(freq))

	hsem <- true
}

type frec struct {
	code  string
	count uint64
}

type frecs []frec

func (a frecs) Len() int           { return len(a) }
func (a frecs) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a frecs) Less(i, j int) bool { return a[i].count < a[j].count }

func getcodes() {

	// Sort by frequency (descending)
	var fr []frec
	for k, v := range freq {
		fr = append(fr, frec{code: k, count: v})
	}
	sort.Sort(sort.Reverse(frecs(fr)))

	// Assign the integer codes so that the most frequent code
	// gets the lowest integer code.
	codes = make(map[string]int)
	for j, f := range fr {
		codes[f.code] = j
	}

	// Save the frequencies
	fn := strings.Replace(codesFile, ".json", "_freq.csv", 1)
	fid, err := os.Create(fn)
	if err != nil {
		panic(err)
	}
	defer fid.Close()
	for _, f := range fr {
		s := fmt.Sprintf("%s,%d\n", f.code, f.count)
		_, err := fid.Write([]byte(s))
		if err != nil {
			panic(err)
		}
	}
}

func Run(files []string, xform xfunc, codesfile string, lgr *log.Logger) {

	logger = lgr
	sem = make(chan bool, concurrency)
	codesFile = codesfile

	xf = xform

	getfreq(files)
	getcodes()

	for _, file := range files {
		sem <- true
		go dofile(file)
	}
	for k := 0; k < concurrency; k++ {
		sem <- true
	}
	logger.Printf("Finished conversions")

	updateDtypes(files)

	// Save the factor code/label associations.
	logger.Printf("Writing code/label associations to %s", codesfile)
	fid, err := os.Create(codesfile)
	if err != nil {
		panic(err)
	}
	defer fid.Close()
	enc := json.NewEncoder(fid)
	err = enc.Encode(codes)
	if err != nil {
		panic(err)
	}

	logger.Printf("All done, exiting")
}