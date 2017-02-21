package boltcluster

// Update is the main interface to interact with the database
func (c *Cluster) Update(distributionKey int, fn BoltDBTxFunction) {
	c.channelFor(distributionKey) <- fn
}

// ParallelView execute transaction function on each database
func (c *Cluster) ParallelUpdate(fn BoltDBTxFunction) {
	for _, ch := range c.channels {
		ch <- fn
	}
}
