package raft

import (
	"log"
	"math/rand"
	"time"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func RandomizeElectionTimer() time.Duration {
	return time.Millisecond*time.Duration(150) + time.Millisecond*time.Duration(rand.Int()%150)
}

func RandomizeHeartBeatenTimer() time.Duration {
	return time.Duration(100) * time.Millisecond
}

func Min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func Max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
