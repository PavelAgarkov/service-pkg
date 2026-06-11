package btree

import (
	"bytes"
	"math/rand"
	"sync"
	"testing"
	"time"
)

// удобные хелперы для тестов

func newTestFilter(key string, ts time.Time) *FilterNodeItem {
	return NewFilterNodeItem([]byte(key), ts)
}

func newTestValue(key, val string, ts time.Time) *ValueNodeItem {
	return NewValueNodeItem([]byte(key), []byte(val), ts)
}

func keysFromItems(items []Item) []string {
	out := make([]string, 0, len(items))
	for _, it := range items {
		out = append(out, string(it.Key()))
	}
	return out
}

func sortedEqualStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	m := make(map[string]int, len(a))
	for _, x := range a {
		m[x]++
	}
	for _, x := range b {
		m[x]--
		if m[x] < 0 {
			return false
		}
	}
	for _, v := range m {
		if v != 0 {
			return false
		}
	}
	return true
}

// ============================================================================
// Базовые операции: Upsert / Has / GetLastWriteUnix / GetNodeItem
// ============================================================================

func TestBTree_UpsertAndHas_FilterItem(t *testing.T) {
	t.Parallel()

	bt := NewByteKeyBTree(Options{})
	ts := time.Unix(1000, 0)
	it := newTestFilter("a", ts)

	if added := bt.Upsert(it); !added {
		t.Fatalf("expected Upsert to report new item")
	}

	// Has по новому Item с тем же ключом, но другим ts
	probe := newTestFilter("a", ts.Add(10*time.Second))
	if !bt.Has(probe) {
		t.Fatalf("Has must be true for existing key")
	}

	// GetLastWriteUnix возвращает исходный ts
	got, ok := bt.GetLastWriteUnix(probe)
	if !ok || got != ts.Unix() {
		t.Fatalf("GetLastWriteUnix: got (%d,%v), want (%d,true)", got, ok, ts.Unix())
	}
}

func TestBTree_UpsertAndHas_ValueItem(t *testing.T) {
	t.Parallel()

	bt := NewByteKeyBTree(Options{})
	ts := time.Unix(2000, 0)
	it := newTestValue("k", "payload", ts)

	if added := bt.Upsert(it); !added {
		t.Fatalf("expected Upsert to report new item")
	}

	if !bt.Has(newTestValue("k", "", ts.Add(time.Minute))) {
		t.Fatalf("Has must be true for existing key")
	}

	gotTs, ok := bt.GetLastWriteUnix(newTestValue("k", "", ts))
	if !ok || gotTs != ts.Unix() {
		t.Fatalf("GetLastWriteUnix: got (%d,%v), want (%d,true)", gotTs, ok, ts.Unix())
	}

	// Проверяем, что value сохранился
	found, ok := bt.GetNodeItem(newTestValue("k", "", ts))
	if !ok {
		t.Fatalf("GetNodeItem: not found")
	}
	v, ok := found.(*ValueNodeItem)
	if !ok {
		t.Fatalf("GetNodeItem: expected *ValueNodeItem")
	}
	if !bytes.Equal(v.Value(), []byte("payload")) {
		t.Fatalf("Value mismatch: got %q, want %q", v.Value(), "payload")
	}
}

func TestBTree_UpsertMany_CountsOnlyNew(t *testing.T) {
	t.Parallel()

	bt := NewByteKeyBTree(Options{})
	base := time.Unix(3000, 0)

	items := []Item{
		newTestFilter("a", base),
		newTestFilter("b", base),
		newTestFilter("", base),                   // пустой ключ — игнор
		newTestFilter("b", base.Add(time.Second)), // дубликат
		newTestValue("c", "x", base),
		nil, // nil — игнор
	}

	added := bt.UpsertMany(items)
	if added != 3 { // a,b,c
		t.Fatalf("UpsertMany added=%d, want 3", added)
	}
	if size := bt.Size(); size != 3 {
		t.Fatalf("Size=%d, want 3", size)
	}

	// повторная вставка тех же ключей — добавлений быть не должно
	added2 := bt.UpsertMany(items)
	if added2 != 0 {
		t.Fatalf("UpsertMany on same keys added=%d, want 0", added2)
	}
}

func TestBTree_DeleteAndDeleteMany(t *testing.T) {
	t.Parallel()

	bt := NewByteKeyBTree(Options{})
	now := time.Unix(4000, 0)

	bt.Upsert(newTestFilter("a", now))
	bt.Upsert(newTestFilter("b", now))
	bt.Upsert(newTestFilter("c", now))

	if ok := bt.Delete(newTestFilter("b", now)); !ok {
		t.Fatalf("Delete(b) must be true")
	}
	if bt.Has(newTestFilter("b", now)) {
		t.Fatalf("b must be deleted")
	}

	n := bt.DeleteMany([]Item{
		newTestFilter("x", now), // нет
		newTestFilter("a", now), // есть
		nil,                     // игнор
		newTestFilter("c", now), // есть
	})
	if n != 2 {
		t.Fatalf("DeleteMany deleted=%d, want 2", n)
	}
	if bt.Size() != 0 {
		t.Fatalf("tree must be empty after deletions, size=%d", bt.Size())
	}
}

func TestBTree_GetNodeItem_ReturnsSamePointer(t *testing.T) {
	t.Parallel()

	bt := NewByteKeyBTree(Options{})
	ts := time.Unix(5000, 0)

	original := NewFilterNodeItem([]byte("key"), ts)
	bt.Upsert(original)

	found, ok := bt.GetNodeItem(NewFilterNodeItem([]byte("key"), ts))
	if !ok {
		t.Fatalf("GetNodeItem: not found")
	}
	ptr, ok := found.(*FilterNodeItem)
	if !ok {
		t.Fatalf("GetNodeItem: expected *FilterNodeItem")
	}
	if ptr != original {
		t.Fatalf("GetNodeItem: pointer changed, want same pointer")
	}
}

func TestBTree_GetLastWriteUnix_MissingKey(t *testing.T) {
	t.Parallel()

	bt := NewByteKeyBTree(Options{})
	ts, ok := bt.GetLastWriteUnix(newTestFilter("absent", time.Now()))
	if ok || ts != 0 {
		t.Fatalf("GetLastWriteUnix on missing key: got (%d,%v), want (0,false)", ts, ok)
	}
}

// ============================================================================
// ForEach / порядок / Size / Reset
// ============================================================================

func TestBTree_ForEach_AscendingOrder(t *testing.T) {
	t.Parallel()

	bt := NewByteKeyBTree(Options{})
	base := time.Unix(6000, 0)

	bt.Upsert(newTestFilter("c", base))
	bt.Upsert(newTestFilter("a", base))
	bt.Upsert(newTestFilter("b", base))

	var seen []string
	err := bt.ForEach(func(key []byte, _ int64) bool {
		seen = append(seen, string(key))
		return true
	})
	if err != nil {
		t.Fatalf("ForEach error: %v", err)
	}

	want := []string{"a", "b", "c"}
	if !sortedEqualStrings(seen, want) || !(seen[0] == "a" && seen[1] == "b" && seen[2] == "c") {
		t.Fatalf("ForEach order=%v, want %v", seen, want)
	}
}

func TestBTree_ForEach_StopEarly(t *testing.T) {
	t.Parallel()

	bt := NewByteKeyBTree(Options{})
	base := time.Unix(7000, 0)

	for _, k := range []string{"a", "b", "c"} {
		bt.Upsert(newTestFilter(k, base))
	}

	var seen []string
	err := bt.ForEach(func(key []byte, _ int64) bool {
		seen = append(seen, string(key))
		return len(seen) < 2 // остановимся после двух
	})
	if err != nil {
		t.Fatalf("ForEach error: %v", err)
	}
	if len(seen) != 2 {
		t.Fatalf("ForEach early stop: got %d items, want 2", len(seen))
	}
}

func TestBTree_SizeAndReset(t *testing.T) {
	t.Parallel()

	bt := NewByteKeyBTree(Options{})

	bt.Upsert(newTestFilter("x", time.Now()))
	bt.Upsert(newTestFilter("y", time.Now()))

	if sz := bt.Size(); sz != 2 {
		t.Fatalf("Size before Reset=%d, want 2", sz)
	}
	bt.Reset()
	if sz := bt.Size(); sz != 0 {
		t.Fatalf("Size after Reset=%d, want 0", sz)
	}
}

// ============================================================================
// TTL: ListExpiredAt / PurgeExpiredAt
// ============================================================================

func TestBTree_TTL_ListExpired_BoundaryInclusive(t *testing.T) {
	t.Parallel()

	bt := NewByteKeyBTree(Options{})

	base := time.Unix(8000, 0)
	ttl := 30 * time.Second
	cutoff := base.Add(-ttl).Unix()

	data := []struct {
		key string
		ts  int64
	}{
		{"a", base.Unix() - 100}, // старый
		{"b", base.Unix() - 50},  // старый
		{"c", cutoff},            // ровно на границе → тоже истёк
		{"d", base.Unix() + 100}, // свежий
	}
	for _, d := range data {
		bt.Upsert(newTestFilter(d.key, time.Unix(d.ts, 0)))
	}

	expired := bt.ListExpiredAt(base, ttl, 0)
	got := keysFromItems(expired)

	if len(expired) != 3 {
		t.Fatalf("expired count=%d, want 3", len(expired))
	}
	if !sortedEqualStrings(got, []string{"a", "b", "c"}) {
		t.Fatalf("expired keys=%v, want [a b c]", got)
	}
}

func TestBTree_TTL_ListExpired_ZeroTTLReturnsNil(t *testing.T) {
	t.Parallel()

	bt := NewByteKeyBTree(Options{})
	now := time.Now()

	bt.Upsert(newTestFilter("x", now.Add(-time.Hour)))

	if res := bt.ListExpiredAt(now, 0, 10); res != nil {
		t.Fatalf("ListExpiredAt with ttl=0 must return nil, got %v", res)
	}
	if deleted := bt.PurgeExpiredAt(now, -1, 0); deleted != 0 {
		t.Fatalf("PurgeExpiredAt with ttl<=0 must delete 0, got %d", deleted)
	}
}

func TestBTree_TTL_PurgeExpired_RemovesOnlyOld(t *testing.T) {
	t.Parallel()

	bt := NewByteKeyBTree(Options{})

	base := time.Unix(9000, 0)
	ttl := 30 * time.Second
	cutoff := base.Add(-ttl).Unix()

	bt.Upsert(newTestFilter("a", time.Unix(base.Unix()-100, 0))) // истёк
	bt.Upsert(newTestFilter("b", time.Unix(base.Unix()-50, 0)))  // истёк
	bt.Upsert(newTestFilter("c", time.Unix(cutoff, 0)))          // истёк
	bt.Upsert(newTestFilter("d", time.Unix(base.Unix()+100, 0))) // свежий

	deleted := bt.PurgeExpiredAt(base, ttl, 0)
	if deleted != 3 {
		t.Fatalf("PurgeExpiredAt deleted=%d, want 3", deleted)
	}

	if bt.Has(newTestFilter("a", base)) ||
		bt.Has(newTestFilter("b", base)) ||
		bt.Has(newTestFilter("c", base)) {
		t.Fatalf("expired items must be removed")
	}
	if !bt.Has(newTestFilter("d", base)) {
		t.Fatalf("fresh item d must remain")
	}
}

func TestBTree_TTL_ListExpired_LimitAndOrder(t *testing.T) {
	t.Parallel()

	bt := NewByteKeyBTree(Options{})

	base := time.Unix(10_000, 0)
	ttl := 1 * time.Hour

	for _, k := range []string{"d", "a", "c", "b"} {
		bt.Upsert(newTestFilter(k, base.Add(-2*time.Hour))) // все истёкшие
	}

	expired := bt.ListExpiredAt(base, ttl, 2)
	if len(expired) != 2 {
		t.Fatalf("ListExpiredAt limited count=%d, want 2", len(expired))
	}
	got := keysFromItems(expired)
	if !(got[0] == "a" && got[1] == "b") {
		t.Fatalf("ListExpiredAt limited order=%v, want [a b]", got)
	}
}

func TestBTree_ListExpired_ReturnsLivePointers(t *testing.T) {
	t.Parallel()

	bt := NewByteKeyBTree(Options{})

	base := time.Unix(11_000, 0)
	ttl := 10 * time.Second

	// делаем один явно истёкший
	old := NewFilterNodeItem([]byte("k"), base.Add(-time.Minute))
	bt.Upsert(old)

	// получаем «expired»
	exp := bt.ListExpiredAt(base, ttl, 0)
	if len(exp) != 1 {
		t.Fatalf("expected exactly 1 expired item, got %d", len(exp))
	}

	// меняем timestamp напрямую → теперь не должен считаться истёкшим
	exp[0].(*FilterNodeItem).timestampUnixSeconds = base.Add(time.Minute).Unix()

	again := bt.ListExpiredAt(base, ttl, 0)
	if len(again) != 0 {
		t.Fatalf("expected 0 expired after raising timestamp, got %d", len(again))
	}
}

// ============================================================================
// Смешивание типов FilterNodeItem / ValueNodeItem
// ============================================================================

func TestBTree_MixedTypes_HasCompatible(t *testing.T) {
	t.Parallel()

	bt := NewByteKeyBTree(Options{})

	ts := time.Unix(12_000, 0)

	bt.Upsert(newTestFilter("k1", ts))
	bt.Upsert(newTestValue("k2", "v", ts))

	if !bt.Has(newTestValue("k1", "", ts)) {
		t.Fatalf("Has must find filter key k1 via value-probe")
	}
	if !bt.Has(newTestFilter("k2", ts)) {
		t.Fatalf("Has must find value key k2 via filter-probe")
	}
}

func TestBTree_MixedTypes_OrderByKey(t *testing.T) {
	t.Parallel()

	bt := NewByteKeyBTree(Options{})

	ts := time.Unix(13_000, 0)

	bt.Upsert(newTestValue("b", "v", ts))
	bt.Upsert(newTestFilter("a", ts))
	bt.Upsert(newTestFilter("c", ts))

	var seen []string
	_ = bt.ForEach(func(key []byte, _ int64) bool {
		seen = append(seen, string(key))
		return true
	})

	want := []string{"a", "b", "c"}
	if len(seen) != len(want) ||
		seen[0] != "a" || seen[1] != "b" || seen[2] != "c" {
		t.Fatalf("mixed types order=%v, want %v", seen, want)
	}
}

// ============================================================================
// Конкурентные тесты
// ============================================================================

func TestBTree_Concurrent_ReadersAndWriters(t *testing.T) {
	bt := NewByteKeyBTree(Options{})

	keys := make([][]byte, 200)
	for i := 0; i < len(keys); i++ {
		keys[i] = []byte{byte(i)}
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup

	// писатели
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			r := rand.New(rand.NewSource(seed))
			for {
				select {
				case <-stop:
					return
				default:
				}
				k := keys[r.Intn(len(keys))]
				bt.Upsert(NewFilterNodeItem(k, time.Unix(14_000, 0)))
			}
		}(int64(1000 + w))
	}

	// читатели
	for rID := 0; rID < 4; rID++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				_ = bt.ForEach(func(_ []byte, _ int64) bool { return true })
				_ = bt.Size()
			}
		}()
	}

	time.Sleep(200 * time.Millisecond)
	close(stop)
	wg.Wait()

	if bt.Size() == 0 {
		t.Fatalf("Concurrent_ReadersAndWriters: tree must not be empty")
	}
}

func TestBTree_Concurrent_PurgeWhileWriting(t *testing.T) {
	bt := NewByteKeyBTree(Options{})

	now := time.Unix(15_000, 0)
	ttl := 2 * time.Second
	cutoffUnix := now.Add(-ttl).Unix()

	// стартовые данные: часть старые, часть почти свежие
	for i := 0; i < 100; i++ {
		ts := now.Add(time.Duration(-5+i%4) * time.Second) // часть < cutoff, часть > cutoff
		bt.Upsert(NewFilterNodeItem([]byte{byte(i)}, ts))
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup

	// писатель: делает элементы явно свежими
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			for i := 0; i < 10; i++ {
				bt.Upsert(NewFilterNodeItem([]byte{byte(i)}, now.Add(5*time.Second)))
			}
		}
	}()

	// чистильщик
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(5 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				bt.PurgeExpiredAt(now, ttl, 10)
			}
		}
	}()

	time.Sleep(200 * time.Millisecond)
	close(stop)
	wg.Wait()

	// Проверяем, что не осталось элементов со старым ts
	err := bt.ForEach(func(_ []byte, tsUnix int64) bool {
		if tsUnix <= cutoffUnix {
			t.Fatalf("found expired item after purges: ts=%d cutoff=%d", tsUnix, cutoffUnix)
		}
		return true
	})
	if err != nil {
		t.Fatalf("ForEach error: %v", err)
	}
}

func TestBTree_Concurrent_ListExpiredDuringWrites(t *testing.T) {
	bt := NewByteKeyBTree(Options{})

	now := time.Unix(16_000, 0)
	ttl := 1 * time.Second

	// все изначально старые
	for i := 0; i < 100; i++ {
		bt.Upsert(NewFilterNodeItem([]byte{byte(i)}, now.Add(-2*time.Second)))
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup

	// писатель освежает случайные ключи
	wg.Add(1)
	go func() {
		defer wg.Done()
		r := rand.New(rand.NewSource(999))
		for {
			select {
			case <-stop:
				return
			default:
			}
			k := []byte{byte(r.Intn(100))}
			bt.Upsert(NewFilterNodeItem(k, now.Add(2*time.Second)))
		}
	}()

	// несколько горутин, которые дергают ListExpiredAt с лимитом
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				out := bt.ListExpiredAt(now, ttl, 7)
				if len(out) > 7 {
					t.Fatalf("ListExpiredAt limit violated: got %d > 7", len(out))
				}
			}
		}()
	}

	time.Sleep(200 * time.Millisecond)
	close(stop)
	wg.Wait()
}

func TestBTree_Concurrent_DeleteDuringIteration(t *testing.T) {
	bt := NewByteKeyBTree(Options{})

	ts := time.Unix(17_000, 0)
	for i := 0; i < 200; i++ {
		bt.Upsert(NewFilterNodeItem([]byte{byte(i)}, ts))
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup

	// итератор
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			_ = bt.ForEach(func(_ []byte, _ int64) bool { return true })
		}
	}()

	// удалятор
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			bt.Delete(NewFilterNodeItem([]byte{byte(i)}, ts))
		}
	}()

	time.Sleep(100 * time.Millisecond)
	close(stop)
	wg.Wait()
}
