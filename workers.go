package boltcluster

import (
	"sync"

	"github.com/boltdb/bolt"
)

func (c *Cluster) startListningToChannels() {
	defer c.wg.Done()

	var wwg sync.WaitGroup
	c.Logger.Println("start filtering workers to listen")
	for i, ch := range c.channels {
		wwg.Add(1)
		go c.dbWorker(i, c.dbs[i], ch, &wwg)
	}

	wwg.Wait()
	c.Logger.Println("startListningToChannels Done")

}

func (c *Cluster) dbWorker(clusterIndex int, db *bolt.DB, in TransactionFunctionChan, wwg *sync.WaitGroup) {
	defer wwg.Done()

	atShutdown := false

	for {

		err := db.Batch(func(tx *bolt.Tx) error {

			for i := 0; i < transactionLimitSize; i++ {

				fn, ok := <-in

				if !ok {
					atShutdown = true
					break
				}

				err := fn(tx)
				if err != nil {
					return err
				}

			}

			return nil

		})

		if err != nil {
			c.Logger.Fatal(err)
		}

		if atShutdown {
			break
		}

	}

}
