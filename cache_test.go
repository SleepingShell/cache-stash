package cache

import (
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
)

func TestCacheNoExpiry(t *testing.T) {
	cache, err := NewCache(EXPIRE_NONE, 0, 0)
	if err != nil {
		t.Error(err)
	}

	first := cache.Get("1")
	if first != nil {
		t.Error("Getting a value in empty initialized cache", first)
	}

	cache.Set("1", 50, NoExpiration)
	cache.Set("2", "Second value", NoExpiration)

	type TestStruct struct {
		d   int
		str string
	}

	//Expiration passed should not matter
	cache.Set("3", TestStruct{d: 1, str: "hello"}, 500*time.Millisecond)
	time.Sleep(1 * time.Second)

	res1 := cache.Get("1")
	if res1 == nil {
		t.Error("Res1 is nil")
	} else if tmp, ok := res1.(int); !ok || tmp != 50 {
		t.Error("Error casting Res1 or incorrect value")
	}

	res2 := cache.Get("2")
	if res2 == nil {
		t.Error("Res2 is nil")
	} else if tmp, ok := res2.(string); !ok || tmp != "Second value" {
		t.Error("Error casting Res2 or incorrect value")
	}

	res3 := cache.Get("3")
	if res3 == nil {
		t.Error("Res3 is nil")
	} else if tmp, ok := res3.(TestStruct); !ok || tmp.d != 1 || tmp.str != "hello" {
		t.Error("Error casting Res3 or incorrect value")
	}
}

func TestCacheRigidExpiry(t *testing.T) {
	logrus.SetLevel(logrus.DebugLevel)
	cache, err := NewCache(EXPIRE_RIGID, 500*time.Millisecond, 2*time.Second)
	if err != nil {
		t.Error(err)
	}

	cache.Set("1", "Hello World", 2*time.Second)

	time.Sleep(1 * time.Second)
	res := cache.Get("1")
	if res == nil {
		t.Error("Premature cleanup")
	}

	time.Sleep(3 * time.Second)
	res = cache.Get("1")
	if res != nil {
		t.Error("Late cleanup")
	}
}

func TestCacheFluidExpiry(t *testing.T) {
	logrus.SetLevel(logrus.DebugLevel)
	cache, err := NewCache(EXPIRE_FLUID, 500*time.Millisecond, 1*time.Second)
	if err != nil {
		t.Error(err)
	}

	cache.Set("1", "Hello World", 2*time.Second)
	time.Sleep(1 * time.Second)
	cache.Get("1")

	time.Sleep(1 * time.Second)
	res := cache.Get("1")
	if res == nil {
		t.Error("Premature cleanup")
	}
	time.Sleep(3 * time.Second)
	res = cache.Get("1")
	if res != nil {
		t.Error("Late cleanup")
	}
}

func TestCacheMap(t *testing.T) {
	m := make(map[int]int)

	m[0] = 50
	m[1] = 100

	cache, err := NewCache(EXPIRE_NONE, 500*time.Millisecond, 2*time.Second)
	if err != nil {
		t.Error(err)
	}
	cache.Set("map", m, NoExpiration)

	recv := cache.Get("map")
	recvMap, ok := recv.(map[int]int)
	if !ok {
		t.Error("Incorrect value returned for map")
	}
	if recvMap[0] != 50 {
		t.Errorf("Incorrect values within received map %d: %d", 0, recvMap[0])
	}

	if recvMap[1] != 100 {
		t.Errorf("Incorrect values within received map %d: %d", 1, recvMap[1])
	}
}

func BenchmarkCacheInserts(b *testing.B) {
	b.Run("NoExpiry", func(b *testing.B) {
		benchmarkCacheInsert(EXPIRE_NONE, 0, 0, 0, b)
	})
	b.Run("RigidExpiry-Consistent", func(b *testing.B) {
		benchmarkCacheInsert(EXPIRE_RIGID, 40*time.Millisecond, 80*time.Millisecond, 60*time.Millisecond, b)
	})
	b.Run("RigidExpiry-Random", func(b *testing.B) {
		benchmarkCacheInsert(EXPIRE_RIGID, 40*time.Millisecond, 80*time.Millisecond, -1, b)
	})
	b.Run("FluidExpiry-Consistent", func(b *testing.B) {
		benchmarkCacheInsert(EXPIRE_FLUID, 40*time.Millisecond, 80*time.Millisecond, 60*time.Millisecond, b)
	})
	b.Run("FluidExpiry-Random", func(b *testing.B) {
		benchmarkCacheInsert(EXPIRE_NONE, 40*time.Millisecond, 80*time.Millisecond, -1, b)
	})
}

func benchmarkCacheInsert(mode CachingMode, cInterval, sInterval time.Duration, itemExp time.Duration, b *testing.B) {
	cache, _ := NewCache(mode, cInterval, sInterval)
	durations := make([]time.Duration, b.N)
	for i := 0; i < b.N; i++ {
		if itemExp > 0 {
			durations[i] = itemExp
		} else if itemExp == -1 {
			durations[i] = time.Duration(rand.Intn(200)) * time.Millisecond
		}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.Set(strconv.Itoa(i), i+2, durations[i])
	}
}
