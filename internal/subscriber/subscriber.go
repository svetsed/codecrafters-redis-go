package subscriber

import (
	"sync"

	"github.com/codecrafters-io/redis-starter-go/internal/model"
)

type Subscribers struct {
	queue map[string][]*model.Client
	mu    sync.RWMutex
}

func NewSubscribers() *Subscribers {
	return &Subscribers{
		queue: map[string][]*model.Client{},
	}
}

func (sb *Subscribers) TryGet(key string) bool {
	sb.mu.RLock()
	defer sb.mu.RUnlock()

	_, exist := sb.queue[key]
	return exist
}

func (sb *Subscribers) Get(key string) (*model.Client, bool) {
	sb.mu.Lock()
	defer sb.mu.Unlock()

	clients, exist := sb.queue[key]
	if !exist {
		return nil, false
	}

	if len(clients) == 0 {
		delete(sb.queue, key)
		return nil, false
	}

	removedClient := clients[0]
	removedClient.Mu.Lock()
	delete(removedClient.SubscribedKeys, key)
	removedClient.Mu.Unlock()

	if len(clients) == 1 {
		delete(sb.queue, key)
		return removedClient, true
	}

	sb.queue[key] = clients[1:]
	return removedClient, true
}

func (sb *Subscribers) Append(client *model.Client, key string) {
	sb.mu.Lock()
	defer sb.mu.Unlock()
	sb.queue[key] = append(sb.queue[key], client)

	client.Mu.Lock()
	client.SubscribedKeys[key] = struct{}{}
	client.Mu.Unlock()
}

func (sb *Subscribers) RemoveClient(client *model.Client, key string) bool {
	sb.mu.Lock()
	defer sb.mu.Unlock()

	clients, exist := sb.queue[key]
	if !exist {
		return false
	}

	newList := make([]*model.Client, 0, len(clients))
	for _, cl := range clients {
		if cl == client {
			continue
		}
		newList = append(newList, cl)
	}

	if len(newList) == 0 {
		delete(sb.queue, key)
	} else {
		sb.queue[key] = newList
	}

	client.Mu.Lock()
	delete(client.SubscribedKeys, key)
	client.Mu.Unlock()

	return true
}

func (sb *Subscribers) UnsubscribeAll(client *model.Client) {
	client.Mu.RLock()
	keys := make([]string, 0, len(client.SubscribedKeys))
	for k := range client.SubscribedKeys {
		keys = append(keys, k)
	}
	client.Mu.RUnlock()

	for _, key := range keys {
		sb.RemoveClient(client, key)
	}
}
