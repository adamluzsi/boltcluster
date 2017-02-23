package boltcluster_test

import (
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/LxDB/boltcluster"
	"github.com/LxDB/convert"
	"github.com/boltdb/bolt"
)

var once sync.Once
var subject *boltcluster.Cluster
var distributionKey int = 1

var verboseCluster bool

func init() {
	if strings.ToLower(os.Getenv("VERBOSE")) == "true" {
		verboseCluster = true
	}
}

func setUp(t *testing.T) {
	once.Do(func() {
		subject = boltcluster.New()

		if verboseCluster {
			subject.Logger.Verbosity = true
		}

		err := subject.Open()
		if err != nil {
			t.Log(err)
			t.Fail()
		}
	})
}

func TestOptions(t *testing.T) {

	newDirectoryPath := "./dbstest"

	if _, err := os.Stat(newDirectoryPath); !os.IsNotExist(err) {
		os.RemoveAll(newDirectoryPath)
	}

	boltcluster.New(boltcluster.SetDirectoryPathTo(newDirectoryPath))

	if _, err := os.Stat(newDirectoryPath); os.IsNotExist(err) {
		t.Log("passing directory path as options does not created the db folder on initialization")
		t.Fail()
	}

}

func TestUpdate(t *testing.T) {
	setUp(t)

	expectedValue := "World"

	err := subject.Update(distributionKey, func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists(convert.Stob(`testing`))

		if err != nil {
			t.Fail()
		}

		bucket.Put(convert.Stob("hello"), convert.Stob(expectedValue))

		return nil
	})

	if err != nil {
		t.Log(err)
		t.Fail()
	}

	var result []byte

	err = subject.Update(distributionKey, func(tx *bolt.Tx) error {
		bucket := tx.Bucket(convert.Stob(`testing`))
		result = convert.Copy(bucket.Get(convert.Stob("hello")))
		return nil
	})

	if err != nil {
		t.Log(err)
		t.Fail()
	}

	resultstr := string(result)

	if resultstr != expectedValue {
		t.Logf("expected %v got %v", expectedValue, resultstr)
		t.Fail()
	}

}

func TestBatch(t *testing.T) {
	setUp(t)

	expectedValue := "World"

	err := subject.Batch(distributionKey, func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists(convert.Stob(`testing`))

		if err != nil {
			t.Fail()
		}

		bucket.Put(convert.Stob("hello"), convert.Stob(expectedValue))

		return nil
	})

	if err != nil {
		t.Log(err)
		t.Fail()
	}

	var result []byte

	err = subject.Batch(distributionKey, func(tx *bolt.Tx) error {
		bucket := tx.Bucket(convert.Stob(`testing`))
		result = convert.Copy(bucket.Get(convert.Stob("hello")))
		return nil
	})

	if err != nil {
		t.Log(err)
		t.Fail()
	}

	resultstr := string(result)

	if resultstr != expectedValue {
		t.Logf("expected %v got %v", expectedValue, resultstr)
		t.Fail()
	}

}

func TestResizeCluster(t *testing.T) {
	newDirectoryPath := "./dbstest"

	if _, err := os.Stat(newDirectoryPath); !os.IsNotExist(err) {
		os.RemoveAll(newDirectoryPath)
	}

	c := boltcluster.New(boltcluster.SetDirectoryPathTo(newDirectoryPath))
	err := c.Open()

	if err != nil {
		t.Log(err)
		t.Fail()
	}

	if verboseCluster {
		c.Logger.Verbosity = true
	}

	c.Update(distributionKey, func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists(convert.Stob(`testing`))
		if err != nil {
			t.Fail()
		}
		bucket.Put(convert.Stob("hello"), convert.Stob("world"))

		return nil
	})

	c.Close()

	c.RedistributeTo(10, func(tx *bolt.Tx) error {
		tx.ForEach(func(k []byte, b *bolt.Bucket) error {
			b.ForEach(func(key, value []byte) error {
				if value != nil {

					somethingThatBeingUsedAsDistributionKey := distributionKey
					bName := convert.Copy(k)
					kName := convert.Copy(key)
					vName := convert.Copy(value)

					c.Batch(somethingThatBeingUsedAsDistributionKey, func(t *bolt.Tx) error {

						bucket, err := t.CreateBucketIfNotExists(bName)

						if err != nil {
							return err
						}

						err = bucket.Put(kName, vName)

						if err != nil {
							return err
						}

						return nil

					})
				}
				return nil
			})
			return nil
		})

		return nil
	})

	var value string
	c.Update(distributionKey, func(tx *bolt.Tx) error {

		bucket, err := tx.CreateBucketIfNotExists(convert.Stob(`testing`))

		if err != nil {
			t.Fail()
		}

		value = string(bucket.Get(convert.Stob("hello")))
		return nil
	})

	if value != "world" {
		t.Log("Distribution failed")
		t.Fail()
	}

	c.Close()

}

func TestParallelUpdate(t *testing.T) {
	newDirectoryPath := "./pupdate"

	if _, err := os.Stat(newDirectoryPath); !os.IsNotExist(err) {
		os.RemoveAll(newDirectoryPath)
	}

	c := boltcluster.New(boltcluster.SetDirectoryPathTo(newDirectoryPath))
	c.RedistributeTo(2, func(_ *bolt.Tx) error { return nil })
	defer c.Close()

	c.Update(1, func(tx *bolt.Tx) error {

		bucket, err := tx.CreateBucketIfNotExists(convert.Stob(`testing`))
		if err != nil {
			t.Fail()
		}

		bucket.Put(convert.Stob("hello"), convert.Itob8(1))
		return nil
	})

	c.Update(2, func(tx *bolt.Tx) error {

		bucket, err := tx.CreateBucketIfNotExists(convert.Stob(`testing`))
		if err != nil {
			t.Fail()
		}

		bucket.Put(convert.Stob("hello"), convert.Itob8(2))
		return nil

	})

	ch := make(chan int)
	var m sync.Mutex
	set := make(map[int]struct{})

	go func() {
		for i := range ch {
			m.Lock()
			set[i] = struct{}{}
			m.Unlock()
		}
	}()

	var wg sync.WaitGroup
	for index := 0; index < 1000; index++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			errs := c.ParallelUpdate(func(tx *bolt.Tx) error {

				bucket := tx.Bucket(convert.Stob(`testing`))

				if bucket != nil {
					by := bucket.Get(convert.Stob("hello"))
					ch <- convert.Btoi(by)
				}

				return nil

			})

			if len(errs) != 0 {
				panic(errs[0])
			}

		}()

		wg.Wait()

		m.Lock()
		setLength := len(set)
		m.Unlock()

		if setLength == 2 {
			break
		} else {
			time.Sleep(500 * time.Millisecond)
		}

	}

	close(ch)

	if len(set) != 2 {
		t.Log("Failed to assert the expected result set is equal")
		t.Log(set)
		t.Fail()
	}

}

func TestParallelBatch(t *testing.T) {
	newDirectoryPath := "./pbatch"

	if _, err := os.Stat(newDirectoryPath); !os.IsNotExist(err) {
		os.RemoveAll(newDirectoryPath)
	}

	c := boltcluster.New(boltcluster.SetDirectoryPathTo(newDirectoryPath))
	c.RedistributeTo(2, func(_ *bolt.Tx) error { return nil })
	defer c.Close()

	c.Update(1, func(tx *bolt.Tx) error {

		bucket, err := tx.CreateBucketIfNotExists(convert.Stob(`testing`))
		if err != nil {
			t.Fail()
		}

		bucket.Put(convert.Stob("hello"), convert.Itob8(1))
		return nil
	})

	c.Update(2, func(tx *bolt.Tx) error {

		bucket, err := tx.CreateBucketIfNotExists(convert.Stob(`testing`))
		if err != nil {
			t.Fail()
		}

		bucket.Put(convert.Stob("hello"), convert.Itob8(2))
		return nil

	})

	ch := make(chan int)
	var m sync.Mutex
	set := make(map[int]struct{})

	go func() {
		for i := range ch {
			m.Lock()
			set[i] = struct{}{}
			m.Unlock()
		}
	}()

	var wg sync.WaitGroup
	for index := 0; index < 1000; index++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			errs := c.ParallelBatch(func(tx *bolt.Tx) error {

				bucket := tx.Bucket(convert.Stob(`testing`))

				if bucket != nil {
					by := bucket.Get(convert.Stob("hello"))
					ch <- convert.Btoi(by)
				}

				return nil

			})

			if len(errs) != 0 {
				panic(errs[0])
			}

		}()

		wg.Wait()

		m.Lock()
		setLength := len(set)
		m.Unlock()

		if setLength == 2 {
			break
		} else {
			time.Sleep(500 * time.Millisecond)
		}

	}

	close(ch)

	if len(set) != 2 {
		t.Log("Failed to assert the expected result set is equal")
		t.Log(set)
		t.Fail()
	}

}

func TestView(t *testing.T) {
	newDirectoryPath := "./pview"

	if _, err := os.Stat(newDirectoryPath); !os.IsNotExist(err) {
		os.RemoveAll(newDirectoryPath)
	}

	c := boltcluster.New(boltcluster.SetDirectoryPathTo(newDirectoryPath))
	c.RedistributeTo(2, func(_ *bolt.Tx) error { return nil })
	defer c.Close()

	c.Update(1, func(tx *bolt.Tx) error {

		bucket, err := tx.CreateBucketIfNotExists(convert.Stob(`testing`))
		if err != nil {
			t.Fail()
		}

		bucket.Put(convert.Stob("hello"), convert.Itob8(42))
		return nil
	})

	c.Update(2, func(tx *bolt.Tx) error {

		bucket, err := tx.CreateBucketIfNotExists(convert.Stob(`testing`))
		if err != nil {
			t.Fail()
		}

		bucket.Put(convert.Stob("hello"), convert.Itob8(32))
		return nil

	})

	ch := make(chan int)
	var m sync.Mutex
	set := make(map[int]struct{})

	go func() {
		for i := range ch {
			m.Lock()
			set[i] = struct{}{}
			m.Unlock()
		}
	}()

	var wg sync.WaitGroup
	for index := 0; index < 1000; index++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			err := c.View(1, func(tx *bolt.Tx) error {

				bucket := tx.Bucket(convert.Stob(`testing`))

				if bucket != nil {
					by := bucket.Get(convert.Stob("hello"))
					ch <- convert.Btoi(by)
				}

				return nil

			})

			if err != nil {
				t.Log(err)
				t.Fail()
			}

		}()

		wg.Wait()

		m.Lock()
		setLength := len(set)
		m.Unlock()

		if setLength == 1 {
			break
		} else {
			time.Sleep(500 * time.Millisecond)
		}

	}

	close(ch)

	if len(set) != 1 {
		t.Log("Failed to assert the expected result set is equal")
		t.Log(set)
		t.Fail()
	}

	if _, ok := set[42]; !ok {
		t.Log("Value not matching")
		t.Log(set)
		t.Fail()
	}

}
