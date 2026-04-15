package storage

import (
	"errors"

	bolt "go.etcd.io/bbolt"
)

var ErrBucketNotFound = errors.New("bucket not found")

// Store is the top-level database handle.
//
// The whole system uses one database file.
type Store struct {
	db *bolt.DB
}

// Tx represents one statement-scoped transaction.
//
// - SELECT uses a read-only transaction
// - DDL and write DML use a write transaction
// - table buckets store: encoded RID -> encoded row
// - catalog and index buckets live in the same database
type Tx struct {
	tx *bolt.Tx
}

// Open opens the single database file.
func Open(path string) (*Store, error) {
	db, err := bolt.Open(path, 0o600, nil)
	if err != nil {
		return nil, err
	}

	return &Store{db: db}, nil
}

// Close releases store resources.
func (s *Store) Close() error {
	if s == nil || s.db == nil {
		return nil
	}

	return s.db.Close()
}

// View runs a read-only transaction.
func (s *Store) View(fn func(*Tx) error) error {
	return s.db.View(func(tx *bolt.Tx) error {
		return fn(&Tx{tx: tx})
	})
}

// Update runs a read-write transaction.
func (s *Store) Update(fn func(*Tx) error) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		return fn(&Tx{tx: tx})
	})
}

// CreateBucket creates a bucket if it does not already exist.
func (tx *Tx) CreateBucket(name string) error {
	_, err := tx.tx.CreateBucketIfNotExists([]byte(name))
	return err
}

// DeleteBucket removes a bucket.
func (tx *Tx) DeleteBucket(name string) error {
	return tx.tx.DeleteBucket([]byte(name))
}

// Put writes one key/value pair into a bucket.
func (tx *Tx) Put(bucketName string, key, value []byte) error {
	bucket, err := tx.bucket(bucketName)
	if err != nil {
		return err
	}

	return bucket.Put(key, value)
}

// Get reads one value from a bucket by key.
func (tx *Tx) Get(bucketName string, key []byte) ([]byte, error) {
	bucket, err := tx.bucket(bucketName)
	if err != nil {
		return nil, err
	}

	value := bucket.Get(key)
	if value == nil {
		return nil, nil
	}

	result := make([]byte, len(value))
	copy(result, value)
	return result, nil
}

// Delete removes one key/value pair from a bucket.
func (tx *Tx) Delete(bucketName string, key []byte) error {
	bucket, err := tx.bucket(bucketName)
	if err != nil {
		return err
	}

	return bucket.Delete(key)
}

// ForEach scans every key/value pair in a bucket in key order.
func (tx *Tx) ForEach(bucketName string, fn func(key, value []byte) error) error {
	bucket, err := tx.bucket(bucketName)
	if err != nil {
		return err
	}

	return bucket.ForEach(func(key, value []byte) error {
		keyCopy := make([]byte, len(key))
		copy(keyCopy, key)

		valueCopy := make([]byte, len(value))
		copy(valueCopy, value)

		return fn(keyCopy, valueCopy)
	})
}

func (tx *Tx) bucket(name string) (*bolt.Bucket, error) {
	bucket := tx.tx.Bucket([]byte(name))
	if bucket == nil {
		return nil, ErrBucketNotFound
	}

	return bucket, nil
}
