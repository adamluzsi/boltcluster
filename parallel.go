package boltcluster

import (
	"sync"

	"github.com/LxDB/bolt"
)

// -----------------------------------Parallel-----------------------------------

// ParallelBatch execute batch transaction function on each database
func (c *Cluster) ParallelBatch(fn BoltDBTxFunction) []error {
	var wg sync.WaitGroup

	errChannel := make(chan error)
	errs := make([]error, 0)

	go func() {
		for err := range errChannel {
			errs = append(errs, err)
		}
	}()

	for _, db := range c.dbs {
		wg.Add(1)

		go func(f BoltDBTxFunction, d *bolt.DB, w *sync.WaitGroup) {
			defer w.Done()

			err := d.Batch(f)

			if err != nil {
				errChannel <- err
			}

		}(fn, db, &wg)
	}

	wg.Wait()
	close(errChannel)

	return errs
}

// ParallelUpdate execute update transaction function on each database
func (c *Cluster) ParallelUpdate(fn BoltDBTxFunction) []error {
	var wg sync.WaitGroup

	errChannel := make(chan error)
	errs := make([]error, 0)

	go func() {
		for err := range errChannel {
			errs = append(errs, err)
		}
	}()

	for _, db := range c.dbs {
		wg.Add(1)

		go func(f BoltDBTxFunction, d *bolt.DB, w *sync.WaitGroup) {
			defer w.Done()

			err := d.Update(f)

			if err != nil {
				errChannel <- err
			}

		}(fn, db, &wg)
	}

	wg.Wait()
	close(errChannel)

	return errs
}

// ParallelView execute view transaction function on each database
func (c *Cluster) ParallelView(fn BoltDBTxFunction) []error {
	var wg sync.WaitGroup

	errChannel := make(chan error)
	errs := make([]error, 0)

	go func() {
		for err := range errChannel {
			errs = append(errs, err)
		}
	}()

	for _, db := range c.dbs {
		wg.Add(1)

		go func(f BoltDBTxFunction, d *bolt.DB, w *sync.WaitGroup) {
			defer w.Done()

			err := d.View(f)

			if err != nil {
				errChannel <- err
			}

		}(fn, db, &wg)
	}

	wg.Wait()
	close(errChannel)

	return errs
}
