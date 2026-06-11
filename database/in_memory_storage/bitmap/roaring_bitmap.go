package bitmap

import (
	bytes2 "bytes"
	"context"
	"fmt"
	"sync"
	"time"

	loggerwrapper "github.com/PavelAgarkov/service-pkg/logger"
	logger "github.com/PavelAgarkov/service-pkg/logger/zap_engine"
	"github.com/PavelAgarkov/service-pkg/utils"

	"github.com/RoaringBitmap/roaring/roaring64"
)

type roaringBitmapStorage struct {
	configs BitmapStorageConfigs
	//эффективно справляется до 100+ миллионов элементов без шардирования.
	//Так же нужно будет шардирование, если  будет высокая конкуренция на запись
	bitmap *roaring64.Bitmap
	mu     sync.RWMutex

	replicator MemorySetStorageReplicator // репликатор для репликации данных в запасное хранилище
	warmer     *Warmer                    // функция, которая будет вызвана для заполнения хранилища
}

type BitmapStorageConfigs struct {
	MonitoringTicker  time.Duration
	OptimizingTicker  time.Duration
	ReplicationTicker time.Duration
	ReplicationTtl    time.Duration
	StorageName       string
	DebugLogs         bool   // флаг для включения/отключения отладочных логов
	ReplicationKey    string // ключ для репликации, например, "bitmap_current_goods_ids"
}

func NewBitmapStorage(
	replicator MemorySetStorageReplicator,
	configs BitmapStorageConfigs,
	warmer *Warmer,
) MemorySetStorage {
	if warmer.BatchSize <= 0 {
		panic(fmt.Sprintf("[%s] warmer batch size must be greater than 0", configs.StorageName))
	}
	s := &roaringBitmapStorage{
		bitmap:     roaring64.NewBitmap(),
		configs:    configs,
		replicator: replicator,
		warmer:     warmer,
	}

	return s
}

func (s *roaringBitmapStorage) MustWarmer(ctx context.Context, warmerFunc WarmerFunc) {
	if s.warmer == nil {
		panic(fmt.Sprintf("[%s] warmer function cannot be nil", s.configs.StorageName))
	}
	if s.warmer.WarmCallback != nil {
		panic(fmt.Sprintf("[%s] warmer function already set", s.configs.StorageName))
	}
	s.warmer.WarmCallback = warmerFunc
	s.background(ctx, s.configs)
}

func (s *roaringBitmapStorage) Contains(key uint64) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	hit := s.bitmap.Contains(key)
	if hit && s.withDebugLogs() {
		logger.WriteDebugLog(context.Background(), &loggerwrapper.LogEntry{
			Msg:       fmt.Sprintf("[%s] contains key %d: %t", s.configs.StorageName, key, hit),
			Component: "roaringBitmapStorage",
			Method:    "Contains",
		})
	}
	return hit
}

func (s *roaringBitmapStorage) UpsertMany(keys []uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.bitmap.AddMany(keys)
	if s.withDebugLogs() {
		logger.WriteDebugLog(context.Background(), &loggerwrapper.LogEntry{
			Msg:       fmt.Sprintf("[%s] upserted %d keys", s.configs.StorageName, len(keys)),
			Component: "roaringBitmapStorage",
			Method:    "UpsertMany",
		})
	}
}

func (s *roaringBitmapStorage) RemoveMany(keys []uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, k := range keys {
		s.bitmap.Remove(k)
	}
	if s.withDebugLogs() {
		logger.WriteDebugLog(context.Background(), &loggerwrapper.LogEntry{
			Msg:       fmt.Sprintf("[%s] removed %d keys", s.configs.StorageName, len(keys)),
			Component: "roaringBitmapStorage",
			Method:    "RemoveMany",
		})
	}
}

func (s *roaringBitmapStorage) GetCount() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.bitmap.GetCardinality()
}

func (s *roaringBitmapStorage) Clear() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.bitmap.Clear()
	if s.withDebugLogs() {
		logger.WriteDebugLog(context.Background(), &loggerwrapper.LogEntry{
			Msg:       fmt.Sprintf("[%s] cleared roaring64 bitmap storage", s.configs.StorageName),
			Component: "roaringBitmapStorage",
			Method:    "Clear",
		})
	}
}

// Warm заполняет хранилище данными. Логика прогревания зависит от того, пустое ли хранилище и решение принимается
// в функции warmer, которая должна быть установлена заранее с помощью MustWarmer
func (s *roaringBitmapStorage) Warm(ctx context.Context) error {
	isEmpty := s.isEmpty()

	if s.warmer == nil {
		panic(fmt.Sprintf("[%s] warmer function cannot be nil", s.configs.StorageName))
	}

	if isEmpty {
		if s.withDebugLogs() {
			logger.WriteDebugLog(ctx, &loggerwrapper.LogEntry{
				Msg:       fmt.Sprintf("[%s] storage is empty, starting warmer function", s.configs.StorageName),
				Component: "roaringBitmapStorage",
				Method:    "Warm",
			})
		}
		data, err := s.warmer.WarmCallback(ctx, s.warmer.BatchSize, s)
		if err != nil {
			return err
		}
		if s.withDebugLogs() {
			logger.WriteDebugLog(ctx, &loggerwrapper.LogEntry{
				Msg:       fmt.Sprintf("[%s] warmer function returned %d items", s.configs.StorageName, len(data)),
				Component: "roaringBitmapStorage",
				Method:    "Warm",
			})
		}

		if s.withDebugLogs() {
			logger.WriteDebugLog(ctx, &loggerwrapper.LogEntry{
				Msg:       fmt.Sprintf("[%s] upserting data to roaring64 bitmap storage", s.configs.StorageName),
				Component: "roaringBitmapStorage",
				Method:    "Warm",
			})
		}

		if len(data) == 0 {
			logger.WriteDebugLog(ctx, &loggerwrapper.LogEntry{
				Msg:       fmt.Sprintf("[%s] warmer function returned no data, skipping upsert", s.configs.StorageName),
				Component: "roaringBitmapStorage",
				Method:    "Warm",
			})
			return nil
		}
		s.UpsertMany(data)
		if s.withDebugLogs() {
			logger.WriteDebugLog(ctx, &loggerwrapper.LogEntry{
				Msg:       fmt.Sprintf("[%s] storage warmed up successfully", s.configs.StorageName),
				Component: "roaringBitmapStorage",
				Method:    "Warm",
			})
		}
	}
	return nil
}

// ReadFromBuffer считывает данные из буфера и инициализирует этим буфером битовую карту
func (s *roaringBitmapStorage) ReadFromBuffer(ctx context.Context, buffer *bytes2.Buffer) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, err := s.bitmap.ReadFrom(buffer)
	if err != nil {
		logger.WriteErrorLog(ctx, &loggerwrapper.LogEntry{
			Msg:       fmt.Sprintf("[%s] failed to read from buffer", s.configs.StorageName),
			Component: "roaringBitmapStorage",
			Method:    "ReadFromBuffer",
			Args:      fmt.Sprintf("versionKey=%s", s.configs.ReplicationKey),
			Error:     err,
		})
		return 0, err
	}
	if s.withDebugLogs() {
		logger.WriteDebugLog(ctx, &loggerwrapper.LogEntry{
			Msg:       fmt.Sprintf("[%s] read from buffer successfully: %d bytes", s.configs.StorageName, p),
			Component: "roaringBitmapStorage",
			Method:    "ReadFromBuffer",
			Args:      fmt.Sprintf("versionKey=%s", s.configs.ReplicationKey),
		})
	}
	return p, nil
}

// GetBytesFromBitmap возвращает байтовое представление битовой карты
func (s *roaringBitmapStorage) GetBytesFromBitmap() ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.bitmap.IsEmpty() {
		return nil, nil
	}

	bitmapBytes, err := s.bitmap.ToBytes()
	if err != nil {
		return nil, err
	}
	if s.withDebugLogs() {
		logger.WriteDebugLog(context.Background(), &loggerwrapper.LogEntry{
			Msg:       fmt.Sprintf("[%s] bitmap converted to bytes successfully: %d bytes", s.configs.StorageName, len(bitmapBytes)),
			Component: "roaringBitmapStorage",
			Method:    "GetBytesFromBitmap",
			Args:      fmt.Sprintf("versionKey=%s", s.configs.ReplicationKey),
		})
	}

	return bitmapBytes, nil
}

// Recover восстанавливает хранилище из байтового представления, полученного из репликатора
func (s *roaringBitmapStorage) Recover(ctx context.Context) error {
	err := s.replicator.Recover(ctx, s, s.configs.ReplicationKey)
	if err != nil {
		logger.WriteErrorLog(ctx, &loggerwrapper.LogEntry{
			Msg:       fmt.Sprintf("[%s] failed to recover bitmap from bytes", s.configs.StorageName),
			Component: "roaringBitmapStorage",
			Method:    "Recover",
			Args:      fmt.Sprintf("versionKey=%s", s.configs.ReplicationKey),
			Error:     err,
		})
		return err
	}
	if s.withDebugLogs() {
		logger.WriteDebugLog(ctx, &loggerwrapper.LogEntry{
			Msg:       fmt.Sprintf("[%s] bitmap recovered successfully: %s", s.configs.StorageName, s.printSize()),
			Component: "roaringBitmapStorage",
			Method:    "Recover",
			Args:      fmt.Sprintf("versionKey=%s", s.configs.ReplicationKey),
		})
	}
	return err
}

func (s *roaringBitmapStorage) Replicate(ctx context.Context) error {
	err := s.replicator.Replicate(ctx, s, s.configs.ReplicationKey, s.configs.ReplicationTtl)
	if err != nil {
		logger.WriteErrorLog(ctx, &loggerwrapper.LogEntry{
			Msg:       fmt.Sprintf("[%s] failed to replicate bitmap to bytes", s.configs.StorageName),
			Component: "roaringBitmapStorage",
			Method:    "Replicate",
			Args:      fmt.Sprintf("versionKey=%s", s.configs.ReplicationKey),
			Error:     err,
		})
	}
	if s.withDebugLogs() {
		if err == nil {
			logger.WriteDebugLog(ctx, &loggerwrapper.LogEntry{
				Msg:       fmt.Sprintf("[%s] bitmap replicated successfully", s.configs.StorageName),
				Component: "roaringBitmapStorage",
				Method:    "Replicate",
				Args:      fmt.Sprintf("versionKey=%s", s.configs.ReplicationKey),
			})
		}
	}
	return err
}

func (s *roaringBitmapStorage) DropReplicationKey(ctx context.Context) error {
	err := s.replicator.DropReplicationKey(ctx, s.configs.ReplicationKey)
	if err != nil {
		logger.WriteErrorLog(ctx, &loggerwrapper.LogEntry{
			Msg:       fmt.Sprintf("[%s] failed to drop replication key", s.configs.StorageName),
			Component: "roaringBitmapStorage",
			Method:    "DropReplicationKey",
			Args:      fmt.Sprintf("versionKey=%s", s.configs.ReplicationKey),
			Error:     err,
		})
		return err
	}

	return nil
}

func (s *roaringBitmapStorage) background(ctx context.Context, configs BitmapStorageConfigs) {
	utils.GoRecover(
		ctx,
		func(localCtx context.Context) {
			monitoringTicker := time.NewTicker(configs.MonitoringTicker)
			defer monitoringTicker.Stop()
			optimizingTicker := time.NewTicker(configs.OptimizingTicker)
			defer optimizingTicker.Stop()
			replicationTicker := time.NewTicker(configs.ReplicationTicker)
			defer replicationTicker.Stop()

			for {
				select {
				case <-localCtx.Done():
					if s.withDebugLogs() {
						logger.WriteDebugLog(localCtx, &loggerwrapper.LogEntry{
							Msg:       fmt.Sprintf("[%s] background goroutine stopped due to context cancellation", s.configs.StorageName),
							Component: "roaringBitmapStorage",
							Method:    "background",
						})
					}
					return
				case <-monitoringTicker.C:
					logger.WriteInfoLog(localCtx, &loggerwrapper.LogEntry{
						Msg:       s.printSize(),
						Component: "roaringBitmapStorage",
						Method:    "background",
						Args:      fmt.Sprintf("versionKey=%s", s.configs.ReplicationKey),
					})
				case <-optimizingTicker.C:
					s.optimize(localCtx)
				case <-replicationTicker.C:
					err := s.Replicate(localCtx)
					if err != nil {
						logger.WriteErrorLog(localCtx, &loggerwrapper.LogEntry{
							Msg:       fmt.Sprintf("[%s] failed to replicate bitmap", s.configs.StorageName),
							Component: "roaringBitmapStorage",
							Method:    "background",
							Args:      fmt.Sprintf("versionKey=%s", s.configs.ReplicationKey),
							Error:     err,
						})
					}
				}
			}
		})
}

func (s *roaringBitmapStorage) optimize(ctx context.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.bitmap.RunOptimize()
	if s.withDebugLogs() {
		logger.WriteDebugLog(ctx, &loggerwrapper.LogEntry{
			Msg:       fmt.Sprintf("[%s] optimized roaring64 bitmap storage", s.configs.StorageName),
			Component: "roaringBitmapStorage",
			Method:    "optimize",
		})
	}
}

func (s *roaringBitmapStorage) isEmpty() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.bitmap.IsEmpty()
}

func (s *roaringBitmapStorage) printSize() string {
	bytes := s.getSizeBytes()
	count := s.GetCount()

	mb := float64(bytes) / 1024 / 1024
	str := fmt.Sprintf(
		"[%s] elements=%d → roaring64 size = %.2f MB (≈ %d bytes)",
		s.configs.StorageName, count, mb, bytes,
	)

	return str
}

func (s *roaringBitmapStorage) getSizeBytes() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.bitmap.GetSizeInBytes()
}

func (s *roaringBitmapStorage) withDebugLogs() bool {
	return s.configs.DebugLogs
}
