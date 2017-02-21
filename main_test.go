package boltcluster_test

import (
	"os"
	"strings"
	"sync"
	"testing"

	. "github.com/adamluzsi/boltcluster/testing"

	"github.com/adamluzsi/boltcluster"
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

func TestReadWrite(t *testing.T) {
	setUp(t)

	ch := make(chan []byte)
	expectedValue := "World"

	subject.Update(distributionKey, func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists(boltcluster.Stob(`testing`))

		if err != nil {
			t.Fail()
		}

		bucket.Put(boltcluster.Stob("hello"), boltcluster.Stob(expectedValue))

		return nil
	})

	subject.Update(distributionKey, func(tx *bolt.Tx) error {
		bucket := tx.Bucket(boltcluster.Stob(`testing`))
		ch <- bucket.Get(boltcluster.Stob("hello"))
		return nil
	})

	result := <-ch
	resultstr := string(result)

	if resultstr != expectedValue {
		t.Logf("expected %v got %v", expectedValue, resultstr)
		t.Fail()
	}

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
		bucket, err := tx.CreateBucketIfNotExists(boltcluster.Stob(`testing`))
		if err != nil {
			t.Fail()
		}
		bucket.Put(boltcluster.Stob("hello"), boltcluster.Stob("world"))

		return nil
	})

	c.Close()

	c.RedistributeTo(10, func(tx *bolt.Tx) error {
		tx.ForEach(func(k []byte, b *bolt.Bucket) error {
			b.ForEach(func(key, value []byte) error {
				if value != nil {

					somethingThatBeingUsedAsDistributionKey := distributionKey
					bName := append([]byte{}, k...)
					kName := append([]byte{}, key...)
					vName := append([]byte{}, value...)

					c.Update(somethingThatBeingUsedAsDistributionKey, func(t *bolt.Tx) error {

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

	ch := make(chan string)
	c.Update(distributionKey, func(tx *bolt.Tx) error {

		bucket, err := tx.CreateBucketIfNotExists(boltcluster.Stob(`testing`))

		if err != nil {
			t.Fail()
		}

		value := bucket.Get(boltcluster.Stob("hello"))

		ch <- string(value)

		return nil
	})

	v := <-ch

	if v != "world" {
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

		bucket, err := tx.CreateBucketIfNotExists(boltcluster.Stob(`testing`))
		if err != nil {
			t.Fail()
		}

		bucket.Put(boltcluster.Stob("hello"), boltcluster.Itob8(1))
		return nil
	})

	c.Update(2, func(tx *bolt.Tx) error {

		bucket, err := tx.CreateBucketIfNotExists(boltcluster.Stob(`testing`))
		if err != nil {
			t.Fail()
		}

		bucket.Put(boltcluster.Stob("hello"), boltcluster.Itob8(2))
		return nil
	})

	ch := make(chan int)
	c.ParallelUpdate(func(tx *bolt.Tx) error {

		bucket, err := tx.CreateBucketIfNotExists(boltcluster.Stob(`testing`))

		if err != nil {
			t.Fail()
		}

		by := bucket.Get(boltcluster.Stob("hello"))
		ch <- boltcluster.Btoi(by)
		return nil
	})

	ints := []int{}
	for index := 0; index < 2; index++ {
		ints = append(ints, <-ch)
	}

	if !TestEqInts(ints, []int{1, 2}) {
		t.Log("Failed to assert the expected result set is equal")
		t.Fail()
	}

}
