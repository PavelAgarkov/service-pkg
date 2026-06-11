package btree

import (
	"bytes"
	"errors"
	"sync"
	"time"

	"github.com/google/btree"
)

type (
	TtlBTree interface {
		Upsert(item Item) bool
		UpsertMany(items []Item) int
		Delete(item Item) bool
		DeleteMany(items []Item) int
		Has(item Item) bool
		GetNodeItem(item Item) (Item, bool)
		GetLastWriteUnix(item Item) (int64, bool)
		ForEach(callback func(key []byte, timestampUnixSeconds int64) bool) error
		Size() int
		Reset()
		PurgeExpiredAt(now time.Time, ttl time.Duration, maxToDelete int) int
		ListExpiredAt(now time.Time, ttl time.Duration, maxCount int) []Item
	}

	// Item — элемент дерева: байтовый ключ и значение.
	// Не меняйте полученные ключи и значения "на месте"! Копируйте их при необходимости.
	// Чтобы не сломать инварианты дерева снаружи.
	Item interface {
		Key() []byte
		Value() []byte
		Less(than btree.Item) bool
		GetExpirationTime() int64
	}
)

// Options — параметры инициализации дерева.
type Options struct {
	// Degree — степень B-дерева (обычно 16..64; по умолчанию 32).
	Degree int
	// FreeListCapacity — максимальное количество узлов B-дерева,
	// которое кэшируется во внутреннем списке свободных узлов (free list).
	// Это НЕ байты. Чем больше значение, тем меньше аллокаций/GC при вставках/удалениях
	FreeListCapacity int
}

// ByteKeyBTree — потокобезопасное B-дерево для байтовых ключей.
type ByteKeyBTree struct {
	tree *btree.BTree
	mu   sync.RWMutex
}

type ValueNodeItem struct {
	valueBytes           []byte
	keyBytes             []byte
	timestampUnixSeconds int64
}

func NewValueNodeItem(key, value []byte, expiration time.Time) *ValueNodeItem {
	return &ValueNodeItem{
		keyBytes:             cloneBytes(key),
		valueBytes:           cloneBytes(value),
		timestampUnixSeconds: expiration.Unix(),
	}
}

func (v *ValueNodeItem) Key() []byte {
	return v.keyBytes
}

func (v *ValueNodeItem) Value() []byte {
	return v.valueBytes
}

// Less — порядок для байтовых ключей.
func (v *ValueNodeItem) Less(b btree.Item) bool {
	return bytes.Compare(v.keyBytes, b.(Item).Key()) < 0
}

func (v *ValueNodeItem) GetExpirationTime() int64 {
	return v.timestampUnixSeconds
}

// FilterNodeItem — элемент дерева: ключ и момент последней записи.
type FilterNodeItem struct {
	keyBytes             []byte
	timestampUnixSeconds int64
}

func NewFilterNodeItem(key []byte, expiration time.Time) *FilterNodeItem {
	return &FilterNodeItem{
		keyBytes:             cloneBytes(key),
		timestampUnixSeconds: expiration.Unix(),
	}
}

func (a *FilterNodeItem) Key() []byte {
	return a.keyBytes
}

func (a *FilterNodeItem) Value() []byte {
	return nil
}

// Less — порядок для байтовых ключей.
func (a *FilterNodeItem) Less(b btree.Item) bool {
	return bytes.Compare(a.keyBytes, b.(Item).Key()) < 0
}

func (a *FilterNodeItem) GetExpirationTime() int64 {
	return a.timestampUnixSeconds
}

// NewByteKeyBTree создаёт дерево с указанными опциями.
func NewByteKeyBTree(opts Options) TtlBTree {
	degree := opts.Degree
	if degree <= 0 {
		degree = 32
	}
	if opts.FreeListCapacity <= 0 {
		opts.FreeListCapacity = 80000
	}
	fl := btree.NewFreeList(opts.FreeListCapacity)

	return &ByteKeyBTree{
		tree: btree.NewWithFreeList(degree, fl),
	}
}

// cloneBytes — делает копию ключа
func cloneBytes(src []byte) []byte {
	if len(src) == 0 {
		return nil
	}
	dst := make([]byte, len(src))
	copy(dst, src)
	return dst
}

// Upsert — вставка/обновление с заданным моментом времени.
// Возвращает true, если ключ был новым.
// Не нужно передавать разные типы Item для одного и того же дерева! Будут проблемы с приведением типов.
func (b *ByteKeyBTree) Upsert(item Item) bool {
	if item == nil {
		return false
	}
	if len(item.Key()) == 0 {
		return false
	}

	b.mu.Lock()
	prev := b.tree.ReplaceOrInsert(item)
	b.mu.Unlock()

	return prev == nil
}

// UpsertMany — массовая вставка/обновление с заданным моментом времени.
// Возвращает число реально новых ключей.
func (b *ByteKeyBTree) UpsertMany(items []Item) int {
	if len(items) == 0 {
		return 0
	}
	added := 0

	b.mu.Lock()
	for _, item := range items {
		if item == nil {
			continue
		}
		if len(item.Key()) == 0 {
			continue
		}
		prev := b.tree.ReplaceOrInsert(item)
		if prev == nil {
			added++
		}
	}
	b.mu.Unlock()

	return added
}

// Delete — удаление ключа. Возвращает true, если ключ существовал.
func (b *ByteKeyBTree) Delete(item Item) bool {
	if item == nil {
		return false
	}
	if len(item.Key()) == 0 {
		return false
	}

	b.mu.Lock()
	deleted := b.tree.Delete(item) != nil
	b.mu.Unlock()
	return deleted
}

// DeleteMany — массовое удаление. Возвращает число реально удалённых ключей.
func (b *ByteKeyBTree) DeleteMany(items []Item) int {
	if len(items) == 0 {
		return 0
	}
	deleted := 0

	b.mu.Lock()
	for _, item := range items {
		if item == nil {
			continue
		}
		if len(item.Key()) == 0 {
			continue
		}
		if b.tree.Delete(item) != nil {
			deleted++
		}
	}
	b.mu.Unlock()
	return deleted
}

// Has — проверка наличия ключа.
func (b *ByteKeyBTree) Has(item Item) bool {
	if item == nil {
		return false
	}
	if len(item.Key()) == 0 {
		return false
	}

	b.mu.RLock()
	exists := b.tree.Get(item) != nil
	b.mu.RUnlock()
	return exists
}

func (b *ByteKeyBTree) GetNodeItem(item Item) (Item, bool) {
	if item == nil {
		return nil, false
	}
	if len(item.Key()) == 0 {
		return nil, false
	}

	b.mu.RLock()
	res := b.tree.Get(item)
	b.mu.RUnlock()
	if res == nil {
		return nil, false
	}
	return res.(Item), true
}

// GetLastWriteUnix — получить unix-секунды последней вставки/обновления.
// Возвращает (0,false), если ключ не найден.
func (b *ByteKeyBTree) GetLastWriteUnix(item Item) (int64, bool) {
	if item == nil {
		return 0, false
	}

	b.mu.RLock()
	res := b.tree.Get(item)
	b.mu.RUnlock()
	if res == nil {
		return 0, false
	}
	return res.(Item).GetExpirationTime(), true
}

// ForEach — полный обход по возрастанию ключей.
// Если callback возвращает false — обход останавливается.
func (b *ByteKeyBTree) ForEach(callback func(key []byte, timestampUnixSeconds int64) bool) error {
	if callback == nil {
		return errors.New("nil callback")
	}
	keepGoing := true
	b.mu.RLock()
	b.tree.Ascend(func(x btree.Item) bool {
		it := x.(Item)
		keyCopy := cloneBytes(it.Key())
		keepGoing = callback(keyCopy, it.GetExpirationTime())
		return keepGoing
	})
	b.mu.RUnlock()
	return nil
}

// Size — текущее количество элементов.
func (b *ByteKeyBTree) Size() int {
	b.mu.RLock()
	n := b.tree.Len()
	b.mu.RUnlock()
	return n
}

// Reset — полная очистка дерева.
func (b *ByteKeyBTree) Reset() {
	b.mu.Lock()
	b.tree.Clear(true)
	b.mu.Unlock()
}

// PurgeExpiredAt — удалить ключи, чья последняя запись старше now - ttl.
// Если maxToDelete <= 0 — без лимита. Возвращает число удалённых.
func (b *ByteKeyBTree) PurgeExpiredAt(now time.Time, ttl time.Duration, maxToDelete int) int {
	if ttl <= 0 {
		return 0
	}
	cutoffUnix := now.Add(-ttl).Unix()

	itemsToDelete := make([]Item, 0)
	b.mu.RLock()
	b.tree.Ascend(func(x btree.Item) bool {
		it := x.(Item)
		if it.GetExpirationTime() <= cutoffUnix {
			itemsToDelete = append(itemsToDelete, it)
			if maxToDelete > 0 && len(itemsToDelete) >= maxToDelete {
				return false
			}
		}
		return true
	})
	b.mu.RUnlock()

	if len(itemsToDelete) == 0 {
		return 0
	}

	deleted := 0
	b.mu.Lock()
	for _, item := range itemsToDelete {
		if b.tree.Delete(item) != nil {
			deleted++
		}
	}
	b.mu.Unlock()

	return deleted
}

// ListExpiredAt — возвращает реальные листья дерева, будьте аккуратны и не меняйте их!
// Граница включительна. Ничего не удаляет.
// Если ttl <= 0 — возвращает nil.
// maxCount > 0 — ограничивает количество возвращаемых ключей, 0/отрицательное — без лимита.
func (b *ByteKeyBTree) ListExpiredAt(now time.Time, ttl time.Duration, maxCount int) []Item {
	if ttl <= 0 {
		return nil
	}
	cutoff := now.Add(-ttl).Unix()

	out := make([]Item, 0)
	limit := maxCount > 0

	b.mu.RLock()
	b.tree.Ascend(func(x btree.Item) bool {
		it := x.(Item)
		if it.GetExpirationTime() <= cutoff {
			out = append(out, it)
			if limit && len(out) >= maxCount {
				return false
			}
		}
		return true
	})
	b.mu.RUnlock()

	return out
}
