package main

import (
	"log"
	"os"

	"github.com/kshedden/goclaims/config"
	"github.com/kshedden/goclaims/sastocols"
)

var (
	conf *config.Config

	logger *log.Logger
)

func setupLogger() {

	fid, err := os.Create("sastocols.log")
	if err != nil {
		panic(err)
	}

	logger = log.New(fid, "", log.Lshortfile)
}

func main() {

	setupLogger()

	if len(os.Args) != 2 {
		os.Stderr.WriteString("sastocols: Wrong number of arguments\n\n")
		os.Exit(1)
	}

	logger.Printf("Reading config from %s", os.Args[1])
	conf := config.ReadConfig(os.Args[1])

	sastocols.Run(conf, logger)
}
