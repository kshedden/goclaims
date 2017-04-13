/*
qperson retrieves all data for one person as csv written to stdout.

Usage:

> qperson config.json 1234567

where 1234567 is a MarketScan enrolid.

The data are returned to stdout in text/csv form, with each row
containing the data for one variable.
*/

package main

import (
	"compress/gzip"
	"encoding/binary"
	"fmt"
	"hash/adler32"
	"io"
	"os"
	"path"
	"strconv"
	"strings"

	"github.com/kshedden/goclaims/config"
)

var (
	conf *config.Config

	procdxcodes  map[string]int
	rprocdxcodes map[int]string
)

// getbucket returns the bucket number for a given enrolid.
func getbucket(enrolid uint64) int {

	ha := adler32.New()

	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, enrolid)
	ha.Write(buf)

	return int(ha.Sum32() % conf.NumBuckets)
}

// findrange returns the indices marking the first and last rows for
// the data of the subject of interest.
func findRange(enrolid uint64, pa string) (int, int) {

	fn := path.Join(pa, "enrolid.bin.gz")
	fid, err := os.Open(fn)
	if err != nil {
		panic(err)
	}
	defer fid.Close()
	rdr, err := gzip.NewReader(fid)
	if err != nil {
		panic(err)
	}
	defer rdr.Close()

	i1 := -1
	i2 := -1
	var n int
	for i := 0; ; i++ {
		var x uint64
		err := binary.Read(rdr, binary.LittleEndian, &x)
		if err == io.EOF {
			break
		} else if err != nil {
			panic(err)
		}

		if x == enrolid {
			if i1 == -1 {
				i1 = i
			}

		} else if x > enrolid {
			if i1 != -1 {
				i2 = i
				break
			}
		} else {
			break
		}

		n++
	}

	if i2 == -1 && i1 != -1 {
		i2 = n
	}

	return i1, i2
}

// handlecol extracts the data for the subject of interest from one variable.
func handleCol(vn string, dt string, i1, i2 int, bp string) []string {

	f := fmt.Sprintf("%s.bin.gz", vn)
	f = path.Join(bp, f)

	fid, err := os.Open(f)
	if err != nil {
		panic(err)
	}
	defer fid.Close()
	rdr, err := gzip.NewReader(fid)
	if err != nil {
		panic(err)
	}
	defer rdr.Close()

	var vals []string
	switch dt {
	case "uint8":
		for {
			var x uint8
			err := binary.Read(rdr, binary.LittleEndian, &x)
			if err == io.EOF {
				break
			} else if err != nil {
				panic(err)
			}
			vals = append(vals, fmt.Sprintf("%d", x))
		}
	case "uint16":
		fl := strings.HasPrefix(vn, "dx") || strings.HasPrefix(vn, "proc")
		for {
			var x uint16
			err := binary.Read(rdr, binary.LittleEndian, &x)
			if err == io.EOF {
				break
			} else if err != nil {
				panic(err)
			}
			if fl {
				vals = append(vals, rprocdxcodes[int(x)])
			} else {
				vals = append(vals, fmt.Sprintf("%d", x))
			}
		}
	case "uint32":
		for {
			var x uint32
			err := binary.Read(rdr, binary.LittleEndian, &x)
			if err == io.EOF {
				break
			} else if err != nil {
				panic(err)
			}
			vals = append(vals, fmt.Sprintf("%d", x))
		}
	}

	return vals
}

func main() {

	if len(os.Args) != 3 {
		os.Stderr.WriteString("Usage: qperson config enrolid\n\n")
		os.Exit(1)
	}

	conf = config.ReadConfig(os.Args[1])

	procdxcodes = config.ReadFactorCodes("ProcDxCodes", conf)
	rprocdxcodes = config.RevCodes(procdxcodes)

	z, err := strconv.Atoi(os.Args[2])
	if err != nil {
		panic(err)
	}
	enrolid := uint64(z)

	bucket := getbucket(enrolid)

	dt := config.ReadDtypes(bucket, conf)
	pa := config.BucketPath(bucket, conf)

	i1, i2 := findRange(enrolid, pa)
	m := i2 - i1

	if m == 0 {
		os.Stderr.WriteString("Enrolid not found\n\n")
		return
	}

	fmt.Printf("enrolid,")
	s := fmt.Sprintf("%d", enrolid)
	for i := 0; i < m; i++ {
		os.Stdout.WriteString(s)
		if i < m-1 {
			os.Stdout.WriteString(",")
		}
	}
	fmt.Printf("\n")

	for vn, dt := range dt {
		if vn == "enrolid" {
			continue
		}

		vals := handleCol(vn, dt, i1, i2, pa)

		fmt.Printf(vn + ",")
		for i, v := range vals {
			fmt.Printf("%s", v)
			if i < len(vals)-1 {
				fmt.Printf(",")
			}
		}
		fmt.Printf("\n")
	}
}
