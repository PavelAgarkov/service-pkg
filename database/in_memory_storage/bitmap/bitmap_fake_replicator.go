package bitmap

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

type BitmapFakeReplicator struct {
	mu         sync.RWMutex
	store      map[string][]byte
	forStorage string
}

func NewBitmapFakeReplicator(forStorage string) MemorySetStorageReplicator {
	return &BitmapFakeReplicator{
		store:      make(map[string][]byte),
		forStorage: forStorage,
	}
}

func (r *BitmapFakeReplicator) Replicate(ctx context.Context, storage MemorySetStorage, replicationKey string, ttl time.Duration) error {
	bitmapBytes, err := storage.GetBytesFromBitmap()
	if err != nil {
		return err
	}

	if bitmapBytes == nil {
		return errors.New(fmt.Sprintf("[%s] bitmap is empty for replicate", r.forStorage))
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	buf := make([]byte, len(bitmapBytes))
	copy(buf, bitmapBytes)

	r.store[replicationKey] = buf
	return nil
}

func (r *BitmapFakeReplicator) Recover(ctx context.Context, storage MemorySetStorage, replicationKey string) error {
	r.mu.RLock()
	data, exists := r.store[replicationKey]
	r.mu.RUnlock()

	if !exists || len(data) == 0 {
		return errors.New(fmt.Sprintf("[%s] no data found for replication key: %s", r.forStorage, replicationKey))
	}

	storage.Clear()
	buf := bytes.NewBuffer(data)

	_, err := storage.ReadFromBuffer(ctx, buf)
	return err
}

func (r *BitmapFakeReplicator) DropReplicationKey(ctx context.Context, replicationKey string) error {
	r.mu.Lock()
	delete(r.store, replicationKey)
	r.mu.Unlock()
	return nil
}
