package boltcluster

import "github.com/boltdb/bolt"

// BoltDBTxFunction bolt db transaction function
type BoltDBTxFunction func(*bolt.Tx) error

func (c *Cluster) dbFor(distributionKey int) *bolt.DB {
	return c.dbs[c.clusterIndexBy(distributionKey)]
}

func (c *Cluster) clusterIndexBy(distributionKey int) int {
	return distributionKey % c.size
}
