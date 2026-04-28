package storage

import (
	"errors"
	"net"
	"sync"

	"github.com/codecrafters-io/redis-starter-go/internal/model"
	"github.com/codecrafters-io/redis-starter-go/internal/subscriber"
)

var (
	WrongType 	 = errors.New("WRONGTYPE")
	InvalidInput = errors.New("incorrect input data")
	NoValues	 = errors.New("key does not exist or is empty") 
)

type Storage struct {
	store map[string]*model.Entry
	subs  *subscriber.Subscribers
	mu    sync.RWMutex
}

func NewStorage(subs *subscriber.Subscribers) *Storage {
	return &Storage{
		store: make(map[string]*model.Entry),
		subs: subs,
	}
}

func (s *Storage) GetValue(key string) (model.Entry, bool) {
	s.mu.RLock()
	val, exist := s.store[key]
	s.mu.RUnlock()
	
	if !exist {
		return model.Entry{}, exist
	}

	return *val, exist
}

func (s *Storage) SetValue(key string, val model.Entry) {
	s.mu.Lock()
	defer s.mu.Unlock()
	entry, exist := s.store[key]
	if !exist {
		s.store[key] = &val
		return
	}

	entry.Value = val.Value
	entry.ExpiresAt = val.ExpiresAt
}

func (s *Storage) DeleteValue(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.store, key)
}

func (s *Storage) RPush(key string, values []string) (int, error) {
	if len(values) == 0 || key == "" {
		return 0, InvalidInput
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	entry, exist := s.store[key]
	if !exist {
		newSlice := make([]string, len(values))
		copy(newSlice, values)
		s.store[key] = &model.Entry{Value: newSlice, ExpiresAt: -1}
		return len(values), nil
	}

	list, ok := entry.Value.([]string)
	if !ok {
		return 0, WrongType
	}

	newList := append(list, values...)
	entry.Value = newList
	return len(newList), nil
}

func (s *Storage) LPush(key string, values []string) (int, error) {
	if len(values) == 0 || key == "" {
		return 0, InvalidInput
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	entry, exist := s.store[key]
	if exist {
		list, ok := entry.Value.([]string)
		if !ok {
			return 0, WrongType
		}

		// add in begin
		newList := make([]string, 0, len(list) + len(values))
		newList = append(newList, values...)
		newList = append(newList, list...)
		entry.Value = newList
		return len(newList), nil
	}

	// no list -> creating
	newList := make([]string, len(values))
	copy(newList, values)
	s.store[key] = &model.Entry{Value: newList, ExpiresAt: -1}
	return len(values), nil
}

func (s *Storage) LPop(key string, n int) ([]string, error) {
	if key == "" || n <= 0 {
		return nil, InvalidInput
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	entry, exist := s.store[key]
	if !exist {
		return nil, nil // no key
	}

	list, ok := entry.Value.([]string)
	if !ok {
		return nil, WrongType
	}

	if n > len(list) {
		return nil, InvalidInput
	}

	removed := make([]string, n)
	copy(removed, list[:n])
	if n == len(list) {
		delete(s.store, key)
		// entry.Value = []string{}
	} else {
		entry.Value = list[n:]
	}

	return removed, nil
}

func (s *Storage) LPopFirst(key string) (string, error) {
	if key == "" {
		return "", InvalidInput
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	entry, exist := s.store[key]
	if !exist {
		return "", NoValues
	}

	list, ok := entry.Value.([]string)
	if !ok {
		return "", WrongType
	}

	if len(list) == 0 {
		delete(s.store, key)
		return "", NoValues
	}

	removed := ""
	if len(list) == 1 {
		removed = list[0]
		delete(s.store, key)
		return removed, nil
	} 

	removed = list[0]
	entry.Value = list[1:]

	return removed, nil
}

func (s *Storage) GetLen(key string) (int, bool) {
	s.mu.RLock()
	entry, exist := s.store[key]
	s.mu.RUnlock()
	if !exist {
		return 0, false
	}

	v, ok := entry.Value.([]string)
	if !ok {
		return 0, false
	}

	return len(v), true
}

func (s *Storage) BLPop(conn net.Conn, key string, time int) ([]string, error) {
	if key == "" || time < 0 {
		return nil, InvalidInput
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	remove, err := s.lpopFirstLocked(key)
	if err != nil {
		if !errors.Is(err, NoValues) {
			return nil, err
		}

		// send client in queque
		s.subs.Append(conn, key)
		return nil, nil
	}

	return []string{key, remove}, nil
}

func (s *Storage) lpopFirstLocked(key string) (string, error) {
	entry, exist := s.store[key]
	if !exist {
		return "", NoValues
	}

	list, ok := entry.Value.([]string)
	if !ok {
		return "", WrongType
	}
	
	if len(list) == 0 {
		return "", NoValues
	}
	
	remove := ""
	if len(list) == 1 {
		remove = list[0]
		delete(s.store, key)
		return remove, nil
	}

	remove = list[0]
	entry.Value = list[1:]
	return remove, nil
}