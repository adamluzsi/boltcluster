package boltcluster

import (
	"log"
	"path/filepath"
)

// Options allow to configure the cluster options
type Options interface {
	Configure(config *config)
}

type dirPathSetter struct {
	directoryPath string
}

func (dps *dirPathSetter) Configure(config *config) {
	if dps.directoryPath == "" {
		log.Fatalln("the input directory path is empty!")
	}

	config.directoryPath = dps.directoryPath
}

// SetDirectoryPathTo allow you can Configure where to set the path for the db cluster
func SetDirectoryPathTo(newDirectoryPath string) Options {
	return &dirPathSetter{newDirectoryPath}
}

type config struct {
	directoryPath string
}

func consumeOptions(opts []Options) *config {
	conf := &config{directoryPath: filepath.Join(".", "dbs")}

	for _, opt := range opts {
		opt.Configure(conf)
	}

	return conf
}
