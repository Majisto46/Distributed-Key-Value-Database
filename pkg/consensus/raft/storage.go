package raft

import (
	"encoding/json"
	"os"
	"sync"
)

// Storage abstracts stable storage for persistent Raft state.
type Storage interface {
	SaveState(term uint64, votedFor string, entries []LogEntry) error
	LoadState() (term uint64, votedFor string, entries []LogEntry, err error)
}

// MemoryStorage provides an in-memory thread-safe implementation of Storage.
type MemoryStorage struct {
	mu       sync.RWMutex
	term     uint64
	votedFor string
	entries  []LogEntry
}

// NewMemoryStorage initializes an empty in-memory Raft storage.
func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{
		entries: make([]LogEntry, 0),
	}
}

func (m *MemoryStorage) SaveState(term uint64, votedFor string, entries []LogEntry) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.term = term
	m.votedFor = votedFor
	m.entries = make([]LogEntry, len(entries))
	copy(m.entries, entries)
	return nil
}

func (m *MemoryStorage) LoadState() (uint64, string, []LogEntry, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	entriesCopy := make([]LogEntry, len(m.entries))
	copy(entriesCopy, m.entries)
	return m.term, m.votedFor, entriesCopy, nil
}

// PersistentStateData is the JSON structure saved to disk.
type PersistentStateData struct {
	Term     uint64     `json:"term"`
	VotedFor string     `json:"voted_for"`
	Entries  []LogEntry `json:"entries"`
}

// FileStorage persists Raft state directly to a JSON/binary file on disk.
type FileStorage struct {
	mu   sync.Mutex
	path string
}

// NewFileStorage initializes persistent disk storage for Raft state.
func NewFileStorage(path string) *FileStorage {
	return &FileStorage{
		path: path,
	}
}

func (f *FileStorage) SaveState(term uint64, votedFor string, entries []LogEntry) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	data := PersistentStateData{
		Term:     term,
		VotedFor: votedFor,
		Entries:  entries,
	}

	bytes, err := json.Marshal(data)
	if err != nil {
		return err
	}

	tmpPath := f.path + ".tmp"
	if err := os.WriteFile(tmpPath, bytes, 0644); err != nil {
		return err
	}

	// Atomic rename ensures no half-written state on crash
	return os.Rename(tmpPath, f.path)
}

func (f *FileStorage) LoadState() (uint64, string, []LogEntry, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if _, err := os.Stat(f.path); os.IsNotExist(err) {
		return 0, "", nil, nil
	}

	bytes, err := os.ReadFile(f.path)
	if err != nil {
		return 0, "", nil, err
	}

	var data PersistentStateData
	if err := json.Unmarshal(bytes, &data); err != nil {
		return 0, "", nil, err
	}

	return data.Term, data.VotedFor, data.Entries, nil
}

