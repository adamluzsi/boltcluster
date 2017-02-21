package boltcluster

// Update is the main interface to interact with the database
func (c *Cluster) Update(distributionKey int, fn BoltDBTxFunction) {
	c.channelFor(distributionKey) <- fn
}
