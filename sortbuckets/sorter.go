/*
Package sortbuckets sorts each bucket first by the bucketing variable,
then by the date variable.

TODO: bucketing variable and date variable are not configurable.
*/

package sortbuckets

import (
	"compress/gzip"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path"
	"sort"

	"github.com/kshedden/goclaims/config"
)

const (
	concurrency = 5
)

var (
	conf *config.Config

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

	fn := path.Join(dirname, "enrolid.bin.gz")
	fide, err := os.Open(fn)
	if err != nil {
		panic(err)
	}
	rdre, err := gzip.NewReader(fide)
	if err != nil {
		panic(err)
	}
	defer rdre.Close()

	fn = path.Join(dirname, "svcdate.bin.gz")
	fids, err := os.Open(fn)
	if err != nil {
		panic(err)
	}
	rdrs, err := gzip.NewReader(fids)
	if err != nil {
		panic(err)
	}
	defer rdrs.Close()

	var dvec []drec
	for pos := 0; ; pos++ {
		var x uint64
		err := binary.Read(rdre, binary.LittleEndian, &x)
		if err == io.EOF {
			break
		} else if err != nil {
			panic(err)
		}

		var y uint16
		err = binary.Read(rdrs, binary.LittleEndian, &y)
		if err == io.EOF {
			break
		} else if err != nil {
			panic(err)
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
func reorder(x []byte, ii []int, w int) []byte {

	y := make([]byte, len(x))
	n := len(x) / w

	if len(ii) != n {
		panic("length error")
	}

	for i := 0; i < n; i++ {
		j := ii[i]
		copy(y[w*i:w*(i+1)], x[w*j:w*(j+1)])
	}

	return y
}

// Reorder the data in one file.
func dofile(filename string, ii []int, w int) {

	fid, err := os.Open(filename)
	if err != nil {
		panic(err)
	}
	defer fid.Close()
	rdr, err := gzip.NewReader(fid)
	if err != nil {
		panic(err)
	}
	defer rdr.Close()

	b, err := ioutil.ReadAll(rdr)
	if err != nil {
		panic(err)
	}

	b = reorder(b, ii, w)

	// Save the reordered data
	fid, err = os.Create(filename)
	if err != nil {
		panic(err)
	}
	defer fid.Close()
	wtr := gzip.NewWriter(fid)
	defer wtr.Close()
	_, err = wtr.Write(b)
	if err != nil {
		panic(err)
	}

	logger.Printf("Finishing %s", filename)
}

// Reorder all files in a directory.
func dodir(dirname string) {

	defer func() { <-sem }()

	ii := getorder(dirname)

	dtypes := getdtypes(dirname)

	for vn, dt := range dtypes {

		fn := path.Join(dirname, vn+".bin.gz")

		w, ok := config.DTsize[dt]
		if !ok {
			msg := fmt.Sprintf("Processing %s\nNo size information for dtype %s\n\n",
				vn, dt)
			os.Stderr.WriteString(msg)
			os.Exit(1)
		}

		dofile(fn, ii, w)
	}
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

func Run(cnf *config.Config, dirname string, lgr *log.Logger) {

	conf = cnf
	logger = lgr

	sem = make(chan bool, concurrency)

	for k := 0; k < int(conf.NumBuckets); k++ {
		dirname := path.Join(conf.TargetDir, "Buckets", fmt.Sprintf("%04d", k))
		sem <- true
		go dodir(dirname)
	}

	for k := 0; k < concurrency; k++ {
		sem <- true
	}
}
