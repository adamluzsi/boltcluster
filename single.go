package boltcluster

// ------------------------------------Single------------------------------------

// Batch is the main interface to interact with the database with batch transaction
func (c *Cluster) Batch(distributionKey int, fn BoltDBTxFunction) error {
	return c.dbFor(distributionKey).Batch(fn)
}

// Update is the main interface to interact with the database
func (c *Cluster) Update(distributionKey int, fn BoltDBTxFunction) error {
	return c.dbFor(distributionKey).Update(fn)
}

// View execute a single view transaction function based on a distributionKey
// Not async
func (c *Cluster) View(distributionKey int, fn BoltDBTxFunction) error {
	return c.dbFor(distributionKey).View(fn)
}
