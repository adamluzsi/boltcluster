package boltcluster

import (
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/boltdb/bolt"
)

// RedistributeTo help you to change the db distribution size on the disc
func (c *Cluster) RedistributeTo(newSize int, moveFn BoltDBTxFunction) error {

	if newSize == c.size {
		c.Logger.Println("Cluster is already on the requested size")
		return nil
	}

	if c.state == true {
		msg := "Please close the database connection before Redistribution," +
			" it will require to open during the process!"

		return errors.New(msg)
	}

	c.Logger.Println("This process is not stopable, please be patient until the program finish")
	c.Logger.Println("wait for cluster transfer complete")

	dir, dbPath := filepath.Split(c.directoryPath)
	bkpDirectory := filepath.Join(dir, "bkp_"+dbPath)
	createDBDirectoryIfNotExists(bkpDirectory)

	stampString := strconv.FormatInt(int64(time.Now().Unix()), 10)
	tmpFolder := filepath.Join(bkpDirectory, stampString)
	c.Logger.Println("backup folder: " + tmpFolder)

	os.Rename(c.directoryPath, tmpFolder)
	createDBDirectoryIfNotExists(c.directoryPath)

	c.size = newSize

	c.Logger.Println("open db with the new cluster files")
	err := c.Open()
	if err != nil {
		return err
	}

	c.Logger.Println("Begin to consume the old database files")

	var wg sync.WaitGroup
	filepath.Walk(tmpFolder, func(path string, info os.FileInfo, err error) error {
		if !info.IsDir() {
			wg.Add(1)

			go c.consumeDB(path, moveFn, &wg)
		}
		return nil
	})

	wg.Wait()
	c.Logger.Println("consumers finished migration")

	return nil

}

func (c *Cluster) consumeDB(path string, fn BoltDBTxFunction, wg *sync.WaitGroup) {
	defer wg.Done()

	var db *bolt.DB
	var err error

	for {
		db, err = bolt.Open(path, 0600, nil)
		if err != nil {
			continue
		} else {
			break
		}
	}

	defer db.Close()

	c.Logger.Println("begin to consume " + path + " file")

	verr := db.View(func(tx *bolt.Tx) error {
		err := fn(tx)

		if err != nil {
			return err
		}

		return nil
	})

	if verr != nil {
		c.Logger.Println(verr)
	}

}
