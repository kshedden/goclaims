/*
Remove all temporary files from the bucket directories.
*/

package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strings"

	"github.com/kshedden/goclaims/config"
)

func main() {

	if len(os.Args) != 2 {
		os.Stderr.WriteString("cleanbuckets: Wrong number of arguments\n\n")
		os.Exit(1)
	}

	conf := config.ReadConfig(os.Args[1])

	nf := 0
	nd := 0
	for k := 0; k < int(conf.NumBuckets); k++ {

		px := config.BucketPath(k, conf)

		op := path.Join(px, "orig")
		err := os.RemoveAll(op)
		if err != nil {
			panic(err)
		}
		nd++

		fl, err := ioutil.ReadDir(px)
		if err != nil {
			panic(err)
		}
		for _, f := range fl {
			fn := f.Name()
			if strings.HasSuffix(fn, "_string.bin.sz") {
				fp := path.Join(px, fn)
				err = os.Remove(fp)
				if err != nil {
					panic(err)
				}
				nf++
			}
		}
	}

	msg := fmt.Sprintf("Removed %d files and %d directories\n", nf, nd)
	os.Stdout.WriteString(msg)
}
