// Package cache provides a key-value cache with optional expiration parameters.
// There are currently 3 modes of operation implemented:
//    1. EXPIRE_NONE - Cache items do not expire
//    2. EXPIRE_RIGID - Cache items expire based upon when they are entered into the cache
//    3. EXPIRE_FLUID - Cache items renew their expiration when retrieved
// Currently, storing a map WILL mean that items updated from the original map will also update
// in the cache, as map values are pointers. TBD if this will be changed.
package cache

import (
	"sort"
	"sync"
	"time"

	//sync "github.com/sasha-s/go-deadlock" //Troubleshoot deadlocking
	log "github.com/sirupsen/logrus"
)

//Cacher interface provides a generic interface for a key-value cache
type Cacher interface {
	//Set sets the key to doc regardless if it already exists
	Set(key string, doc interface{}, expiration time.Duration)

	//Get returns the document for a key, nil otherwise
	Get(key string) interface{}

	//GetExpiration returns when the given key expires, and a 0 time if the key doesn't exist
	GetExpiration(key string) time.Time

	//Exists return if a given key exists
	Exists(key string) bool

	//EvictExpired will evict any items that are expired and return the number evicted
	EvictExpired() int
}

//CachingMode specifies how items should expire or be evicted
type CachingMode int

const (
	//EXPIRE_NONE cache items do not expire and expiration parameters are ignored
	EXPIRE_NONE CachingMode = iota

	//EXPIRE_RIGID cache items expiration duration are set at item insertion and not updated
	EXPIRE_RIGID

	//EXPIRE_FLUID cache items expiration is reset on every retrieval
	EXPIRE_FLUID
)

const (
	//NoExpiration specifies the item will not expire
	NoExpiration time.Duration = -1
)

//Item represents an entry, including the expiration info
type Item struct {
	value interface{}

	//expiration is the nanoseconds timestamp when the item will expire, as time.Time is too large (mostly bc of location)
	//TODO: But if location is a pointer, does it really store a lot?
	expiration int64

	//d is the expiration duration
	d time.Duration
}

//IsExpired will return whether an item is past expiration
func (i *Item) IsExpired() bool {
	if i.expiration == 0 {
		return false
	}
	return time.Now().UnixNano() > i.expiration
}

//Value returns the underlying value stored
func (i *Item) Value() interface{} {
	return i.value
}

//Cache is a vanilla key-value store in which items can expire after a given duration
type Cache struct {
	mode  CachingMode
	items map[string]*Item
	mutex sync.RWMutex

	//MAY NOT be same length as map, as some items don't have an expiration
	expTracker *ExpirationTracker

	cleaner *Cleaner
}

//Set will set the key to the given document and expiration
//Pass NoExpiration if the item should be permanent
func (c *Cache) Set(key string, doc interface{}, expiration time.Duration) {
	log.Debug("Setting value")
	var expiry bool
	item := Item{value: doc}
	if expiration == NoExpiration || c.mode == EXPIRE_NONE {
		item.expiration = 0
		expiry = false
	} else {
		item.expiration = time.Now().Add(expiration).UnixNano()
		item.d = expiration
		expiry = true
	}

	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.items[key] = &item
	if expiry {
		c.expTracker.Add(key, item.expiration)
	}
}

//Get returns the underlying document with a key, or nil if it doesn't exist
//If this cache is EXPIRE_FLUID, it will renew the expiration on the item
func (c *Cache) Get(key string) interface{} {
	log.Debug("Getting value")
	c.mutex.RLock()
	item, ok := c.items[key]

	if !ok {
		c.mutex.RUnlock()
		return nil
	}
	c.mutex.RUnlock()
	if c.mode == EXPIRE_FLUID {
		c.mutex.Lock()
		c.items[key].expiration = time.Now().Add(item.d).UnixNano()
		c.expTracker.Add(key, c.items[key].expiration)
		c.mutex.Unlock()
	}

	return item.Value()
}

//GetExpiration returns when the given key expires, and a 0 time if the key doesn't exist
func (c *Cache) GetExpiration(key string) time.Time {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	item, ok := c.items[key]
	if !ok {
		return time.Time{}
	}
	return time.Unix(0, item.expiration)
}

//Exists returns if there is an entry for a key
func (c *Cache) Exists(key string) bool {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	_, ok := c.items[key]
	return ok
}

//EvictExpired will evict all expired entries from the cache and return the number evicted
func (c *Cache) EvictExpired() int {
	log.Debug("Evicting...")
	c.expTracker.Sort()
	c.expTracker.mutex.Lock()
	//defer c.expTracker.mutex.Unlock()
	if c.expTracker.Len() < 1 {
		c.expTracker.mutex.Unlock()
		return 0
	}

	now := time.Now().UnixNano()

	var toDelete []string
	for {
		if c.expTracker.Len() == 0 {
			break
		}
		entry := c.expTracker.arr[0]
		if now > entry.e {
			toDelete = append(toDelete, entry.key)
			//TODO: Move this to a method on ExpirationTracker, but must deal with mutex deadlocking
			c.expTracker.arr = c.expTracker.arr[1:]
		} else {
			break
		}
	}
	c.expTracker.mutex.Unlock()

	//TODO: Problem is getting this lock, also need to fix locking in Set(). Should lock both mutexes in the same order across routines
	c.mutex.Lock()
	defer c.mutex.Unlock()
	i := 0
	for _, key := range toDelete {
		actual, ok := c.items[key]
		if ok {
			if actual.IsExpired() {
				delete(c.items, key)
				i++
			}
		}
	}

	return i
}

//NewCache returns a cache with the given mode
func NewCache(mode CachingMode, cleanInterval, sortInterval time.Duration) *Cache {
	cache := Cache{
		mode:       mode,
		items:      make(map[string]*Item),
		expTracker: &ExpirationTracker{},
	}

	if mode != EXPIRE_NONE {
		//TODO: Check for 0 values
		c := NewCleaner(cleanInterval, sortInterval, cache.expTracker.Sort)
		cache.cleaner = &c
		go cache.cleaner.Run(&cache)
	}

	return &cache
}

//ExpirationTracker provides a concurrent safe array of keys to expire. It will periodically
//be sorted so the soonest to expire entry is first.
//This MAY contain duplicate entries, and the parent Cacher should account for this on eviction
type ExpirationTracker struct {
	arr   []KeyExpiration
	mutex sync.RWMutex
}

//KeyExpiration is an entry in ExpirationTracker specifying when a key expires
type KeyExpiration struct {
	key string
	e   int64
}

//Add will append the key and expiration to the ExpirationTracker. The array should mostly be sorted
//just from insertion, except for varying expiration durations
func (t *ExpirationTracker) Add(key string, e int64) {
	val := KeyExpiration{
		key: key,
		e:   e,
	}
	t.mutex.Lock()
	t.arr = append(t.arr, val)
	log.WithField("Len", t.Len()).Debug("Added Expiration tracker")
	t.mutex.Unlock()
}

//Sort sorts the underlying array by expiration time
func (t *ExpirationTracker) Sort() {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	if t.Len() < 2 {
		return
	}
	sort.Sort(t)
	//log.Debug("Sorted", len(t.arr))
}

//Len provides the length of the underlying array
func (t *ExpirationTracker) Len() int {
	return len(t.arr)
}

//Less returns which expiration is soonest
func (t *ExpirationTracker) Less(i, j int) bool {
	return t.arr[i].e < t.arr[j].e
}

//Swap swaps two entries in the array
func (t *ExpirationTracker) Swap(i, j int) {
	t.arr[i], t.arr[j] = t.arr[j], t.arr[i]
}

//Cleaner will periodically evict the expired data
//It will also periodically sort the expiration tracker, so the soonest to expire
//data is at the front of the array.
type Cleaner struct {
	CleanInterval time.Duration
	SortInterval  time.Duration

	//Pass the function that should be called to sort the expiration array
	Sort func()
}

//NewCleaner returns a cleaner with the given intervals
// cInterval - How often to clean (evict) the cache
// sInterval - How often to sort the expiration tracker by calling s
func NewCleaner(cInterval, sInterval time.Duration, s func()) Cleaner {
	return Cleaner{
		CleanInterval: cInterval,
		SortInterval:  sInterval,
		Sort:          s,
	}
}

//Run runs the cleaner rountine that will periodically clean the cache and sort the expiration
//array.
func (c Cleaner) Run(cache Cacher) {
	var cleanTicker, sortTicker *time.Ticker
	cleanTicker = time.NewTicker(c.CleanInterval)
	if c.SortInterval > 0 {
		sortTicker = time.NewTicker(c.SortInterval)
	} else {
		sortTicker = &time.Ticker{}
	}
	for {
		select {
		case <-cleanTicker.C:
			i := cache.EvictExpired()
			log.WithField("Num", i).Debug("Evicted expired")
		case <-sortTicker.C:
			c.Sort()
		}
	}
}
