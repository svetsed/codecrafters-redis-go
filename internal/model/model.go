package model

import (
	"net"
	"sync"
)

type Entry struct {
	Value     any
	ExpiresAt int64 // in milliseconds
}

type Client struct {
    Conn       net.Conn
    WakeUpChan chan *WakeUpData
    SubscribedKeys map[string]struct{}
    Mu             sync.RWMutex
}

type WakeUpData struct {
    Key   string
    Value string
}