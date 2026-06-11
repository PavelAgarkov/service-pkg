package bitmap

import (
	"context"
	"time"
)

type BitmapStubReplicator struct{}

func NewBitmapStubReplicator() MemorySetStorageReplicator {
	return &BitmapStubReplicator{}
}

func (stub *BitmapStubReplicator) Replicate(ctx context.Context, storage MemorySetStorage, replicationKey string, ttl time.Duration) error {
	return nil
}

func (stub *BitmapStubReplicator) Recover(ctx context.Context, storage MemorySetStorage, versionKey string) error {
	return nil
}

func (stub *BitmapStubReplicator) DropReplicationKey(ctx context.Context, replicationKey string) error {
	return nil
}
