package boltcluster

import "errors"

// ErrDatabaseAlreadyOpen is an error that returnted when Open method already initialized db connection
var ErrDatabaseAlreadyOpen = errors.New("Database Already open!")
