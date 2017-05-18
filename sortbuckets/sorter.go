/*
Package sortbuckets sorts each bucket first by an id variable, then
by a date variable.
*/

package sortbuckets

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path"
	"sort"
	"strings"

	"github.com/golang/snappy"
	"github.com/kshedden/gosascols/config"
)

const (
	concurrency = 10
)

var (
	conf *config.Config

	// Name of the identifier variable
	idvar string

	// Name of the date variable
	timevar string

	logger *log.Logger

	// Semaphore to limit concurrency
	sem chan bool
)

type drec struct {
	enrolid uint64
	date    uint16
	pos     int
}

type dslice []drec

func (d dslice) Len() int {
	return len(d)
}

func (d dslice) Less(i, j int) bool {
	if d[i].enrolid < d[j].enrolid {
		return true
	}
	if d[i].enrolid > d[j].enrolid {
		return false
	}
	return d[i].date < d[j].date
}

func (d dslice) Swap(i, j int) {
	d[i], d[j] = d[j], d[i]
}

// Get the sorted order for a bucket based on the enrolid and date variables.
func getorder(dirname string) []int {

	var rdre, rdrs io.Reader

	hasid := idvar != ""
	hastime := timevar != ""

	if !(hasid || hastime) {
		panic("Must sort on at least one of id and time")
	}

	if hasid {
		fn := path.Join(dirname, idvar+".bin.sz")
		fide, err := os.Open(fn)
		if err != nil {
			panic(err)
		}
		rdre = snappy.NewReader(fide)
	}

	if hastime {
		fn := path.Join(dirname, timevar+".bin.sz")
		fids, err := os.Open(fn)
		if err != nil {
			panic(err)
		}
		rdrs = snappy.NewReader(fids)
	}

	var dvec []drec
	for pos := 0; ; pos++ {

		var x uint64
		var y uint16

		if hasid {
			err := binary.Read(rdre, binary.LittleEndian, &x)
			if err == io.EOF {
				break
			} else if err != nil {
				panic(err)
			}
		}

		if hastime {
			err := binary.Read(rdrs, binary.LittleEndian, &y)
			if err == io.EOF {
				break
			} else if err != nil {
				panic(err)
			}
		}

		dvec = append(dvec, drec{x, y, pos})
	}

	sort.Sort(dslice(dvec))

	ii := make([]int, len(dvec))
	for j, v := range dvec {
		ii[j] = v.pos
	}
	return ii
}

// Reorder a slice containing fixed width values of width w, using the
// indices in ii.
func reorderbytes(x []byte, ii []int, w int) []byte {

	y := make([]byte, len(x))
	n := len(x) / w

	if len(ii) != n {
		panic("reorderbytes: length error")
	}

	for i := 0; i < n; i++ {
		j := ii[i]
		copy(y[w*i:w*(i+1)], x[w*j:w*(j+1)])
	}

	return y
}

// Reorder a slice of uint64, using the indices in ii.
func reorderuint64(x []uint64, ii []int) []uint64 {

	if len(ii) != len(x) {
		print(fmt.Sprintf("%d != %d\n", len(x), len(ii)))
		panic("reorderuint64: length error")
	}

	n := len(x)
	y := make([]uint64, n)
	for i := 0; i < n; i++ {
		y[i] = x[ii[i]]
	}

	return y
}

func readbytes(fname string) []byte {
	fid, err := os.Open(fname)
	if err != nil {
		panic(err)
	}
	defer fid.Close()
	rdr := snappy.NewReader(fid)
	b, err := ioutil.ReadAll(rdr)
	if err != nil {
		panic(err)
	}
	return b
}

func readuvarint(fname string) []uint64 {
	fid, err := os.Open(fname)
	if err != nil {
		panic(err)
	}
	defer fid.Close()
	rdr := snappy.NewReader(fid)
	br := bufio.NewReader(rdr)

	var b []uint64
	for {
		u, err := binary.ReadUvarint(br)
		if err == io.EOF {
			break
		} else if err != nil {
			panic(err)
		}
		b = append(b, u)
	}

	return b
}

// Reorder the fixed-width data in one file.
func dofixedwidth(filename string, ii []int, w int) {

	if strings.HasSuffix(filename, "_string.bin.sz") {
		logger.Printf("Skipping %s", filename)
		return
	}

	logger.Printf("Starting file %s", filename)

	d, f := path.Split(filename)
	bname := path.Join(d, "orig", f)
	logger.Printf("Renaming %s -> %s\n", filename, bname)
	err := os.Rename(filename, bname)
	if err != nil {
		panic(err)
	}

	b := readbytes(bname)
	b = reorderbytes(b, ii, w)

	// Save the reordered data
	fid, err := os.Create(filename)
	if err != nil {
		panic(err)
	}
	defer fid.Close()
	wtr := snappy.NewBufferedWriter(fid)
	defer wtr.Close()
	_, err = wtr.Write(b)
	if err != nil {
		panic(err)
	}

	logger.Printf("Finishing file %s", filename)
}

// Reorder variable width data (currently must be uvarint) in one file.
func dovarwidth(filename string, ii []int) {

	if strings.HasSuffix(filename, "_string.bin.sz") {
		logger.Printf("Skipping %s", filename)
		return
	}

	logger.Printf("Starting file %s", filename)

	d, f := path.Split(filename)
	bname := path.Join(d, "orig", f)
	logger.Printf("Renaming %s -> %s\n", filename, bname)
	err := os.Rename(filename, bname)
	if err != nil {
		panic(err)
	}

	b := readuvarint(bname)
	b = reorderuint64(b, ii)

	// Save the reordered data
	fid, err := os.Create(filename)
	if err != nil {
		panic(err)
	}
	defer fid.Close()
	wtr := snappy.NewBufferedWriter(fid)
	defer wtr.Close()
	buf := make([]byte, 8)
	for _, x := range b {
		m := binary.PutUvarint(buf, x)
		_, err = wtr.Write(buf[0:m])
		if err != nil {
			panic(err)
		}
	}

	logger.Printf("Finishing file %s", filename)
}

// Reorder all files in a directory.
func dodir(dirname string) {

	defer func() { <-sem }()

	logger.Printf("Starting directory %s", dirname)

	ii := getorder(dirname)

	dtypes := getdtypes(dirname)

	// Original files are placed in this directory as backups
	err := os.MkdirAll(path.Join(dirname, "orig"), 0755)
	if err != nil {
		panic(err)
	}

	for vn, dt := range dtypes {

		fn := path.Join(dirname, vn+".bin.sz")

		if dt == "uvarint" {
			dovarwidth(fn, ii)
		} else {
			w, ok := config.DTsize[dt]
			if !ok {
				log.Printf("Processing %s\nNo size information for dtype %s\n\n",
					vn, dt)
				os.Exit(1)
			}
			dofixedwidth(fn, ii, w)
		}
	}

	logger.Printf("Finishing directory %s", dirname)
}

func getdtypes(dirname string) map[string]string {
	fid, err := os.Open(path.Join(dirname, "dtypes.json"))
	if err != nil {
		panic(err)
	}
	dtypes := make(map[string]string)
	dec := json.NewDecoder(fid)
	err = dec.Decode(&dtypes)
	if err != nil {
		panic(err)
	}

	return dtypes
}

func Run(cnf *config.Config, id, time, dirname string, lgr *log.Logger) {

	conf = cnf
	logger = lgr

	idvar = id
	timevar = time

	sem = make(chan bool, concurrency)

	for k := 0; k < int(conf.NumBuckets); k++ {
		dirname := config.BucketPath(k, conf)
		sem <- true
		go dodir(dirname)
	}

	for k := 0; k < concurrency; k++ {
		sem <- true
	}
	logger.Printf("Done")
}
