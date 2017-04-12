package main

import (
	"log"
	"os"
	"path"

	"github.com/kshedden/goclaims/config"
	"github.com/kshedden/goclaims/sortbuckets"
)

var (
	logger *log.Logger
)

func setupLogger() {

	fid, err := os.Create("sortbuckets.log")
	if err != nil {
		panic(err)
	}

	logger = log.New(fid, "", log.Lshortfile)
}

func main() {

	setupLogger()

	if len(os.Args) != 2 {
		os.Stderr.WriteString("sortbuckets: wrong number of arguments\n\n")
		os.Exit(1)
	}

	config := config.ReadConfig(os.Args[1])

	dirname := path.Join(config.TargetDir, "Buckets")
	sortbuckets.Run(config, dirname, logger)
}
