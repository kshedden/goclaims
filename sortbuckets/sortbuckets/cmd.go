package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path"

	"github.com/kshedden/goclaims/config"
	"github.com/kshedden/goclaims/sortbuckets"
)

var (
	logger *log.Logger

	conf *config.Config
)

func setupLogger() {

	fn := "sortbuckets_" + path.Base(conf.TargetDir) + ".log"
	fid, err := os.Create(fn)
	if err != nil {
		panic(err)
	}

	logger = log.New(fid, "", log.Ltime)
}

func revert() {

	for k := 0; k < int(conf.NumBuckets); k++ {

		px := config.BucketPath(k, conf)
		py := path.Join(px, "orig")
		fl, err := ioutil.ReadDir(py)
		if os.IsNotExist(err) {
			continue
		} else if err != nil {
			panic(err)
		}
		for _, f := range fl {
			fn := f.Name()
			tod := path.Join(px, fn)
			fro := path.Join(py, fn)
			fmt.Printf("%s -> %s\n", fro, tod)
			err = os.Rename(fro, tod)
			if err != nil {
				panic(err)
			}
		}
	}
}

func main() {

	if len(os.Args) != 5 {
		_, _ = os.Stderr.WriteString("sortbuckets: wrong number of arguments, usage\n\n")
		_, _ = os.Stderr.WriteString("    sortbuckets config.json idvar timevar (run|revert)\n\n")
		os.Exit(1)
	}

	conf = config.ReadConfig(os.Args[1])
	setupLogger()
	logger.Printf("Read configuration from %s", os.Args[1])

	if os.Args[4] == "revert" {
		logger.Printf("Reverting to unsorted state")
		revert()
		os.Exit(0)
	}

	idvar := os.Args[2]
	timevar := os.Args[3]

	if idvar != "" {
		logger.Printf("Sorting on id variable %s", idvar)
	}

	if timevar != "" {
		logger.Printf("Sorting on time variable %s", timevar)
	}

	dirname := path.Join(conf.TargetDir, "Buckets")
	sortbuckets.Run(conf, idvar, timevar, dirname, logger)

	logger.Printf("All done, exiting")
}
