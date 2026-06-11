package bitmap

import (
	bytes2 "bytes"
	"context"
	"time"
)

type (
	WarmerFunc func(ctx context.Context, batchSize int32, bitmap MemorySetStorage) ([]uint64, error)
	Warmer     struct {
		WarmCallback WarmerFunc
		BatchSize    int32
	}

	MemorySetStorage interface {
		// MustWarmer устанавливает функцию, которая будет вызываться для заполнения хранилища
		MustWarmer(ctx context.Context, warmerFunc WarmerFunc)
		// Contains проверяет, содержится ли ключ в хранилище
		Contains(key uint64) bool
		// UpsertMany добавляет несколько ключей в хранилище
		UpsertMany(keys []uint64)
		// RemoveMany удаляет несколько ключей из хранилища
		RemoveMany(keys []uint64)
		// GetCount возвращает количество элементов в хранилище
		GetCount() uint64
		// Clear очищает хранилище
		Clear()
		// Warm заполняет хранилище данными, если оно пустое
		Warm(ctx context.Context) error
		// ReadFromBuffer считывает данные из буфера и добавляет их в bitmap
		ReadFromBuffer(ctx context.Context, buffer *bytes2.Buffer) (int64, error)
		// GetBytesFromBitmap возвращает байтовое представление bitmap
		GetBytesFromBitmap() ([]byte, error)
		// Recover восстанавливает bitmap из байтового представления
		Recover(ctx context.Context) error
		// Replicate реплицирует данные из bitmap в хранилище
		Replicate(ctx context.Context) error
		// DropReplicationKey удаляет ключ репликации из хранилища
		DropReplicationKey(ctx context.Context) error
	}

	MemorySetStorageReplicator interface {
		// Replicate реплицирует данные в хранилище
		Replicate(ctx context.Context, storage MemorySetStorage, replicationKey string, ttl time.Duration) error
		// Recover восстанавливает данные из хранилища
		Recover(ctx context.Context, storage MemorySetStorage, replicationKey string) error
		// DropReplicationKey удаляет ключ репликации из хранилища
		DropReplicationKey(ctx context.Context, replicationKey string) error
	}
)
