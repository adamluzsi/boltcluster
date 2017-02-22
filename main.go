package boltcluster

import (
	"sync"

	"github.com/boltdb/bolt"
)

// Update is the main interface to interact with the database
func (c *Cluster) Update(distributionKey int, fn BoltDBTxFunction) {
	c.channelFor(distributionKey) <- fn
}

// ParallelUpdate execute transaction function on each database
func (c *Cluster) ParallelUpdate(fn BoltDBTxFunction) {
	for _, ch := range c.channels {
		ch <- fn
	}
}

// ParallelView execute view transaction function on each database
func (c *Cluster) ParallelView(fn BoltDBTxFunction) *sync.WaitGroup {
	var wg sync.WaitGroup
	for _, db := range c.dbs {
		wg.Add(1)

		go func(f BoltDBTxFunction, d *bolt.DB, w *sync.WaitGroup) {
			defer w.Done()

			err := d.View(f)
			if err != nil {
				c.Logger.Println(err)
			}

		}(fn, db, &wg)
	}

	return &wg
}
