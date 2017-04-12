package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path"
	"strings"

	"github.com/kshedden/goclaims/config"
	"github.com/kshedden/goclaims/factorize"
)

var (
	conf *config.Config

	logger *log.Logger
)

func getfilenames() []string {
	var files []string
	for k := 0; k < int(conf.NumBuckets); k++ {
		px := fmt.Sprintf("Buckets/%04d", k)
		fl, err := ioutil.ReadDir(path.Join(conf.TargetDir, px))
		if err != nil {
			panic(err)
		}
		for _, f := range fl {
			fn := f.Name()
			if strings.HasSuffix(fn, ".bin.gz") && !strings.HasSuffix(fn, "_orig.bin.gz") &&
				(strings.HasPrefix(fn, "dx") || strings.HasPrefix(fn, "proc")) {
				files = append(files, path.Join(px, fn))
			}
		}
	}

	return files
}

func setupLogger() {

	fid, err := os.Create("factorizedx.log")
	if err != nil {
		panic(err)
	}

	logger = log.New(fid, "", log.Lshortfile)
}

func main() {

	if len(os.Args) != 2 {
		os.Stderr.WriteString("wrong number of arguments\n\n")
		os.Exit(1)
	}

	setupLogger()

	logger.Printf("Reading config file from %s", os.Args[1])
	conf = config.ReadConfig(os.Args[1])

	files := getfilenames()
	logger.Printf("Processing %d files", len(files))

	codefile := "ProcDxCodes.json"
	factorize.Run(conf, files, codefile, logger)
}
