package boltcluster

import (
	"errors"

	"github.com/boltdb/bolt"
)

// BoltDBTxFunction bolt db transaction function
type BoltDBTxFunction func(*bolt.Tx) error

// TransactionFunctionChan is the channel type where
type TransactionFunctionChan chan BoltDBTxFunction

func (c *Cluster) populateChannels() error {
	c.Logger.Println("Populate feeder channels")

	if len(c.dbs) == 0 {
		return errors.New("no DB connection enstabilized yet")
	}

	if len(c.channels) != 0 {
		return errors.New("channels already populated")
	}

	for i := range c.dbs {
		ch := make(TransactionFunctionChan)
		c.channels[i] = ch
	}

	return nil
}

func (c *Cluster) channelFor(distributionKey int) TransactionFunctionChan {
	return c.channels[c.clusterIndexBy(distributionKey)]
}

func (c *Cluster) clusterIndexBy(distributionKey int) int {
	return distributionKey % c.size
}
