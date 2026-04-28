package subscriber

import (
	"net"
	"sync"

)

type Subscribers struct {
	queue map[string][]net.Conn
	mu	   sync.RWMutex
}

func NewSubscribers() *Subscribers {
	return &Subscribers{
		queue: map[string][]net.Conn{},
	}
}

func (sb *Subscribers) TryGet(key string) bool {
	sb.mu.RLock()
	defer sb.mu.RUnlock()

	_, exist := sb.queue[key]
	return exist
}

func (sb *Subscribers) Get(key string) (net.Conn, bool) {
	sb.mu.Lock()
	defer sb.mu.Unlock()

	conns, exist := sb.queue[key]
	if !exist {
		return nil, false
	}
	
	if len(conns) == 0 {
		delete(sb.queue, key)
		return nil, false
	}

	var remove net.Conn
	if len(conns) == 1 {
		remove = conns[0]
		delete(sb.queue, key)
		return remove, true
	}
	
	remove = conns[0]
	sb.queue[key] = conns[1:]
	return remove, true
}

func (sb *Subscribers) Append(conn net.Conn, key string) {
	sb.mu.Lock()
	defer sb.mu.Unlock()
	sb.queue[key] = append(sb.queue[key], conn)
}