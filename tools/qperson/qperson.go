/*
qperson retrieves all data for one person as csv written to stdout.

Usage:

> qperson config.json 1234567

where 1234567 is a MarketScan enrolid.

The data are returned to stdout in text/csv form, with each row
containing the data for one variable.
*/

//go:generate go run gen.go defs.template

package main

import (
	"encoding/binary"
	"fmt"
	"hash/adler32"
	"io"
	"os"
	"path"
	"strconv"
	"strings"

	"github.com/golang/snappy"
	"github.com/kshedden/goclaims/config"
)

var (
	conf *config.Config

	procCodes  map[string]int
	dxCodes    map[string]int
	rprocCodes map[int]string
	rdxCodes   map[int]string
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

	fn := path.Join(pa, "Enrolid.bin.sz")
	fid, err := os.Open(fn)
	if err != nil {
		panic(err)
	}
	defer fid.Close()
	rdr := snappy.NewReader(fid)

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
		}

		n++
	}

	if i2 == -1 && i1 != -1 {
		i2 = n
	}

	return i1, i2
}

func main() {

	if len(os.Args) != 3 {
		os.Stderr.WriteString("Usage: qperson config enrolid\n\n")
		os.Exit(1)
	}

	conf = config.ReadConfig(os.Args[1])

	dxCodes = config.ReadFactorCodes("DxCodes", conf)
	rdxCodes = config.RevCodes(dxCodes)

	procCodes = config.ReadFactorCodes("ProcCodes", conf)
	rprocCodes = config.RevCodes(procCodes)

	z, err := strconv.Atoi(os.Args[2])
	if err != nil {
		panic(err)
	}
	enrolid := uint64(z)

	bucket := getbucket(enrolid)
	fmt.Printf("bucket=%d\n", bucket)

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

		var vals []string
		if dt != "uvarint" {
			vals = handleFixedCol(vn, dt, i1, i2, pa)
		} else {
			var codes map[int]string
			if strings.HasPrefix(vn, "Dx") {
				codes = rdxCodes
			} else {
				codes = rprocCodes
			}
			vals = handleVarCol(vn, dt, i1, i2, pa, codes)
		}

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
