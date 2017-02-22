package boltcluster

import (
	"log"
	"os"
	"strconv"
)

var transactionLimitSize int

const defaultTransactionLimitSize = 1000

func init() {
	transactionLimitSize = defaultTransactionLimitSize

	transactionLimitSizeStr := os.Getenv("TRANSACTION_LIMIT_SIZE")
	if transactionLimitSizeStr != "" {
		newTransactionLimitSize, err := strconv.Atoi(transactionLimitSizeStr)
		if err == nil {
			log.Println("New TRANSACTION_LIMIT_SIZE set to " + transactionLimitSizeStr)
			transactionLimitSize = newTransactionLimitSize
		}
	}

}
