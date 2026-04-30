package model

import "net"

type Entry struct {
	Value     any
	ExpiresAt int64 // in milliseconds
}

type Client struct {
    Conn       net.Conn
    WakeUpChan chan *WakeUpData
}

type WakeUpData struct {
    Key   string
    Value string
}