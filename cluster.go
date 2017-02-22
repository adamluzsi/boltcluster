package boltcluster

import (
	"io/ioutil"

	"github.com/boltdb/bolt"
)

// Cluster object that handles storing new uniq values
type Cluster struct {
	size          int
	dbs           map[int]*bolt.DB
	state         bool
	Logger        *Logger
	directoryPath string
}

// New method Return a new Cluster struct
func New(options ...Options) *Cluster {

	conf := consumeOptions(options)
	directoryPath := conf.directoryPath
	createDBDirectoryIfNotExists(directoryPath)

	files, _ := ioutil.ReadDir(directoryPath)
	size := len(files)

	if size == 0 {
		size = 1
	}

	c := &Cluster{Logger: newLogger(), directoryPath: conf.directoryPath, size: size, state: false}
	c.reset()

	return c
}

func (c *Cluster) reset() {
	c.dbs = make(map[int]*bolt.DB)
	c.state = false
}

// Open connect the cluster to the distributed databases
func (c *Cluster) Open() error {

	c.Logger.Println("Open Database connection")

	if c.state {
		return ErrDatabaseAlreadyOpen
	}

	err := c.connectToDatabases()

	if err != nil {
		return err
	}

	if err != nil {
		return err
	}

	return nil
}

// Close The database connections
func (c *Cluster) Close() error {

	c.Logger.Println("Close database connections")
	for _, db := range c.dbs {
		err := db.Close()
		if err != nil {
			return nil
		}
	}

	c.Logger.Println("All database closed")

	c.reset()
	return nil
}
