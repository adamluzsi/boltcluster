package boltcluster

import (
	"strconv"

	"github.com/boltdb/bolt"
)

func (c *Cluster) connectToDatabases() error {
	c.Logger.Printf("Connect to clustered database (%s)\n", strconv.Itoa(c.size))

	if c.size == 0 {
		c.Logger.Fatalln("Warning: no database in the " + c.directoryPath)
	}

	for index := 0; index < c.size; index++ {
		dbPath := c.directoryPath + "/" + strconv.Itoa(index)
		db, err := bolt.Open(dbPath, 0600, nil)

		if err != nil {
			return err
		}

		c.dbs[index] = db
	}

	c.state = true
	return nil
}
