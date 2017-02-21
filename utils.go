package boltcluster

import "os"

func createDBDirectoryIfNotExists(directoryPath string) {
	if _, err := os.Stat(directoryPath); os.IsNotExist(err) {
		os.Mkdir(directoryPath, os.ModePerm)
	}
}
