package bitmap

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	loggerwrapper "github.com/PavelAgarkov/service-pkg/logger"
	logger "github.com/PavelAgarkov/service-pkg/logger/zap_engine"

	"github.com/go-redis/redis/v8"
)

type BitmapRedisReplicator struct {
	redis      *redis.Client
	forStorage string
}

func NewBimapRedisReplicator(redisClient *redis.Client, forStorage string) MemorySetStorageReplicator {
	if redisClient == nil {
		panic("redis client must be not nil")
	}

	r := &BitmapRedisReplicator{
		redis:      redisClient,
		forStorage: forStorage,
	}

	return r
}

// Replicate реплицирует данные из MemorySetStorage в резервное хранилище (в данном случае в Redis).
func (r *BitmapRedisReplicator) Replicate(ctx context.Context, storage MemorySetStorage, replicationKey string, ttl time.Duration) error {
	bitmapBytes, err := storage.GetBytesFromBitmap()
	if err != nil {
		return err
	}

	if bitmapBytes == nil {
		return errors.New(fmt.Sprintf("[%s] bitmap is empty for replicate", r.forStorage))
	}

	err = r.writeBytesToDump(ctx, replicationKey, bitmapBytes, ttl)
	if err != nil {
		return err
	}

	return nil
}

// Recover восстанавливает данные из резервного хранилища в MemorySetStorage(конкретно тут из redis в roaring64.Bitmap).
func (r *BitmapRedisReplicator) Recover(ctx context.Context, storage MemorySetStorage, versionKey string) error {
	bitmapBytesFromDump, err := r.readBytesFromDump(ctx, versionKey)
	if err != nil {
		return err
	}
	storage.Clear()

	buffer := bytes.NewBuffer(bitmapBytesFromDump)
	_, err = storage.ReadFromBuffer(ctx, buffer)
	if err != nil {
		return err
	}

	return nil
}

func (r *BitmapRedisReplicator) DropReplicationKey(ctx context.Context, replicationKey string) error {
	err := r.redis.Del(ctx, replicationKey).Err()
	if err != nil {
		return err
	}
	return nil
}

// readBytesFromDump получает байтовое представление битовой карты из резервного хранилища (в данном случае из Redis).
func (r *BitmapRedisReplicator) readBytesFromDump(ctx context.Context, versionKey string) ([]byte, error) {
	bitmapBytes, err := r.redis.Get(ctx, versionKey).Bytes()
	if err != nil {
		return nil, err
	}

	if bitmapBytes == nil {
		logger.WriteInfoLog(ctx, &loggerwrapper.LogEntry{
			Msg:       fmt.Sprintf("[%s] bitmap is empty from dump", r.forStorage),
			Component: "BitmapRedisReplicator",
			Method:    "readBytesFromDump",
			Args:      fmt.Sprintf("versionKey=%s", versionKey),
		})
		return nil, nil
	}

	return bitmapBytes, nil
}

// writeBytesToDump отправляет байтовое представление битовой карты в резервное хранилище (в данном случае в Redis).
func (r *BitmapRedisReplicator) writeBytesToDump(ctx context.Context, versionKey string, bytes []byte, ttl time.Duration) error {
	err := r.redis.Set(ctx, versionKey, bytes, ttl).Err()
	if err != nil {
		logger.WriteErrorLog(ctx, &loggerwrapper.LogEntry{
			Msg:       fmt.Sprintf("failed to write bitmap to dump in %s", r.forStorage),
			Component: "BitmapRedisReplicator",
			Method:    "writeBytesToDump",
			Args:      fmt.Sprintf("versionKey=%s, ttl=%s", versionKey, ttl.String()),
			Error:     err,
		})
		return err
	}
	return nil
}
