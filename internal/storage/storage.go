package storage

import (
	"sync"

	"github.com/codecrafters-io/redis-starter-go/internal/model"
)

type Storage struct {
	store map[string]model.Entry
	mu    sync.RWMutex
}

func NewStorage() *Storage {
	return &Storage{
		store: make(map[string]model.Entry),
	}
}

func (s *Storage) GetValue(key string) (model.Entry, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	val, exist := s.store[key]
	return val, exist
}

func (s *Storage) SetValue(key string, val model.Entry) {
	s.mu.Lock()
	s.store[key] = val
	s.mu.Unlock()
}

func (s *Storage) DeleteValue(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.store, key)
}

func (s *Storage) UpdateOrSetValue(key string, newVal model.Entry, lastLen int) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	// if new -> just added
	val, exist := s.store[key]
	if !exist {
		s.store[key] = newVal
		return true
	}

	v, ok := val.Value.([]string)
	if !ok {
		return false
	}

	// check if there have been changes between mutex
	if len(v) == lastLen {
		s.store[key] = newVal
		return true
	}

	nv, ok := newVal.Value.([]string)
	if !ok {
		return false
	}

	// if length has changed, then add it again
	v = append(v, nv...)
	s.store[key] = model.Entry{Value: v, ExpiresAt: newVal.ExpiresAt}
	return true
}
