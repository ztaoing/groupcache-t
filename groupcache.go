/*
Copyright 2012 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package groupcache 提供了一个缓存的数据加载机制，
// and de-duplication that works across a set of peer processes.
//
// 每一个数据都会首先向本地缓存获取数据，否则会向数据的拥有者获取 Get first consults its local cache, otherwise delegates
// to the requested key's canonical owner, which then checks its cache
// or finally gets the data.  In the common case, many concurrent
// cache misses across a set of peers for the same key result in just
// one cache fill.
// 在常见情况下，由于同一个键，一组对等体之间的许多并发缓存未命中只会导致一个缓存填满。
package groupcache_t

import (
	"context"
	"errors"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"

	pb "github.com/golang/groupcache/groupcachepb"
	"github.com/golang/groupcache/lru"
	"github.com/golang/groupcache/singleflight"
)

// 获取key的数据
type Getter interface {
	// Get returns the value identified by key, populating dest.
	// 返回的数据必须是未版本化的。 也就是说，密钥必须唯一地描述加载的数据，没有隐式当前时间，并且不依赖于缓存过期机制。
	Get(ctx context.Context, key string, dest Sink) error
}

// A GetterFunc 实现了Getter接口
type GetterFunc func(ctx context.Context, key string, dest Sink) error

func (f GetterFunc) Get(ctx context.Context, key string, dest Sink) error {
	return f(ctx, key, dest)
}

var (
	mu     sync.RWMutex
	groups = make(map[string]*Group)

	initPeerServerOnce sync.Once
	initPeerServer     func()
)

// GetGroup returns the named group previously created with NewGroup, or
// nil if there's no such group.
func GetGroup(name string) *Group {
	mu.RLock()
	g := groups[name]
	mu.RUnlock()
	return g
}

// NewGroup creates a coordinated group-aware Getter from a Getter.
//
// The returned Getter tries (but does not guarantee) to run only one
// Get call at once for a given key across an entire set of peer
// processes. Concurrent callers both in the local process and in
// other processes receive copies of the answer once the original Get
// completes.
//
// The group name must be unique for each getter.
func NewGroup(name string, cacheBytes int64, getter Getter) *Group {
	return newGroup(name, cacheBytes, getter, nil)
}

// If peers is nil, the peerPicker is called via a sync.Once to initialize it.
func newGroup(name string, cacheBytes int64, getter Getter, peers PeerPicker) *Group {
	if getter == nil {
		panic("nil Getter")
	}
	mu.Lock()
	defer mu.Unlock()
	initPeerServerOnce.Do(callInitPeerServer)
	if _, dup := groups[name]; dup {
		panic("duplicate registration of group " + name)
	}
	g := &Group{
		name:       name,
		getter:     getter,
		peers:      peers,
		cacheBytes: cacheBytes,
		loadGroup:  &singleflight.Group{},
	}
	if fn := newGroupHook; fn != nil {
		fn(g)
	}
	groups[name] = g
	return g
}

// newGroupHook, if non-nil, is called right after a new group is created.
var newGroupHook func(*Group)

// RegisterNewGroupHook registers a hook that is run each time
// a group is created.
func RegisterNewGroupHook(fn func(*Group)) {
	if newGroupHook != nil {
		panic("RegisterNewGroupHook called more than once")
	}
	newGroupHook = fn
}

// RegisterServerStart注册了一个钩子，他会在第一个group被创建的时候运行
func RegisterServerStart(fn func()) {
	if initPeerServer != nil {
		panic("RegisterServerStart called more than once")
	}
	initPeerServer = fn
}

func callInitPeerServer() {
	if initPeerServer != nil {
		initPeerServer()
	}
}

// A Group is a cache namespace and associated data loaded spread over a group of 1 or more machines.
// Group 是一个缓存的命名空间，同时将相关联的数据加载后同步到一组机器（1个或多个机器组成）中
type Group struct {
	name       string
	getter     Getter
	peersOnce  sync.Once
	peers      PeerPicker
	cacheBytes int64 // 用于限制 mainCache和hotCache和的大小

	// mainCache is a cache of the keys for which this process
	// (amongst its peers) is authoritative.
	//That is, this cache contains keys which consistent hash on to this process's peer number.
	// mainCache是一个存储
	// mainCache
	mainCache cache

	// hotCache contains keys/values for which this peer is not
	// authoritative (otherwise they would be in mainCache), but
	// are popular enough to warrant mirroring in this process to
	// avoid going over the network to fetch from a peer.  Having
	// a hotCache avoids network hotspotting, where a peer's
	// network card could become the bottleneck on a popular key.
	// This cache is used sparingly to maximize the total number
	// of key/value pairs that can be stored globally.
	hotCache cache

	// loadGroup ensures that each key is only fetched once
	// (either locally or remotely), regardless of the number of
	// concurrent callers.
	// loadGroup确保了每一个key在同一时刻只能有一个在处理，从而避免了并发调用
	loadGroup flightGroup

	_ int32 // 在32位的平台上强制对齐为8字节

	// Stats 是group的统计信息
	Stats Stats
}

// flightGroup is defined as an interface which flightgroup.Group
// satisfies.  我们对此进行定义，以便我们可以用另一种方法进行测试执行。
type flightGroup interface {
	// 当Do处理完成的时候会调动Done.
	Do(key string, fn func() (interface{}, error)) (interface{}, error)
}

// Stats 是每一个group的统计信息
type Stats struct {
	Gets           AtomicInt // 任何的Get请求，包括来自peers的Get
	CacheHits      AtomicInt // either cache was good 缓存命中
	PeerLoads      AtomicInt // either remote load or remote cache hit (not an error)任何一个远端加载或者远端缓存命中
	PeerErrors     AtomicInt
	Loads          AtomicInt // (gets - cacheHits) 缓存命中的数量
	LoadsDeduped   AtomicInt // after singleflight 重复加载的数
	LocalLoads     AtomicInt // total good local loads 所有成功的本地加载数
	LocalLoadErrs  AtomicInt // total bad local loads 所有失败的本地加载数
	ServerRequests AtomicInt // gets that came over the network from peers 通过网络从peers获得请求的数量
}

// Name returns the name of the group.
func (g *Group) Name() string {
	return g.name
}

func (g *Group) initPeers() {
	if g.peers == nil {
		// 通过group name获取peers
		g.peers = getPeers(g.name)
	}
}

func (g *Group) Get(ctx context.Context, key string, dest Sink) error {
	g.peersOnce.Do(g.initPeers)
	// 将get请求的计数加一
	g.Stats.Gets.Add(1)

	if dest == nil {
		return errors.New("groupcache: nil dest Sink")
	}
	// 通过key查询缓存，返回命中的值和是否命中
	value, cacheHit := g.lookupCache(key)
	if cacheHit {
		// 如果命中了缓存，就将缓存命中的计数加一
		g.Stats.CacheHits.Add(1)
		//并将value保存到destination中
		return setSinkView(dest, value)
	}

	// Optimization to avoid double unmarshalling or copying: keep
	// track of whether the dest was already populated. One caller
	// (if local) will set this; the losers will not. The common
	// case will likely be one caller.
	// 没有命中缓存
	destPopulated := false
	value, destPopulated, err := g.load(ctx, key, dest)
	if err != nil {
		return err
	}
	if destPopulated {
		return nil
	}
	return setSinkView(dest, value)
}

// load 通过调用本地的getter或者通过把它发送到另一个节点来加载key
func (g *Group) load(ctx context.Context, key string, dest Sink) (value ByteView, destPopulated bool, err error) {
	g.Stats.Loads.Add(1)
	viewi, err := g.loadGroup.Do(key, func() (interface{}, error) {
		// Check the cache again because singleflight can only dedup calls
		// that overlap concurrently.  It's possible for 2 concurrent
		// requests to miss the cache, resulting in 2 load() calls.  An
		// unfortunate goroutine scheduling would result in this callback
		// being run twice, serially.  If we don't check the cache again,
		// cache.nbytes would be incremented below even though there will
		// be only one entry for this key.
		// 再次检查缓存，因为singleflight只能删除重复并发的调用。 2个并发请求可能会丢失高速缓存，从而导致2个load（）调用。
		// 不幸的goroutine调度会导致此回调连续运行两次。 如果我们不再次检查缓存，
		//  即使此键只有一个条目，cache.nbytes也会增加到下面。

		// Consider the following serialized event ordering for two
		// goroutines in which this callback gets called twice for the
		// same key:
		// 考虑以下针对两个goroutine的序列化事件顺序，其中针对同一键对该回调调用两次
		// 1: Get("key")
		// 2: Get("key")
		// 1: lookupCache("key")
		// 2: lookupCache("key")
		// 1: load("key")
		// 2: load("key")
		// 1: loadGroup.Do("key", fn)
		// 1: fn()
		// 2: loadGroup.Do("key", fn)
		// 2: fn()
		if value, cacheHit := g.lookupCache(key); cacheHit {
			g.Stats.CacheHits.Add(1)
			return value, nil
		}

		g.Stats.LoadsDeduped.Add(1)

		var value ByteView
		var err error
		// 通过key找到peer
		if peer, ok := g.peers.PickPeer(key); ok {
			// 通过peer找到value
			value, err = g.getFromPeer(ctx, peer, key)
			if err == nil {
				g.Stats.PeerLoads.Add(1)
				return value, nil
			}
			// 没能从peer中获取到数据
			g.Stats.PeerErrors.Add(1)
			// TODO(bradfitz): log the peer's error? keep
			// log of the past few for /groupcachez?  It's
			// probably boring (normal task movement), so not
			// worth logging I imagine.
		}
		// 没能找到peer，就从本地查找
		value, err = g.getLocally(ctx, key, dest)
		if err != nil {
			g.Stats.LocalLoadErrs.Add(1)
			return nil, err
		}
		g.Stats.LocalLoads.Add(1)
		destPopulated = true // only one caller of load gets this return value
		//
		g.populateCache(key, value, &g.mainCache)
		return value, nil
	})
	if err == nil {
		value = viewi.(ByteView)
	}
	return
}

func (g *Group) getLocally(ctx context.Context, key string, dest Sink) (ByteView, error) {
	err := g.getter.Get(ctx, key, dest)
	if err != nil {
		return ByteView{}, err
	}
	return dest.view()
}

func (g *Group) getFromPeer(ctx context.Context, peer ProtoGetter, key string) (ByteView, error) {
	req := &pb.GetRequest{
		Group: &g.name,
		Key:   &key,
	}
	res := &pb.GetResponse{}
	err := peer.Get(ctx, req, res)
	if err != nil {
		return ByteView{}, err
	}
	value := ByteView{b: res.Value}
	// TODO(bradfitz): use res.MinuteQps or something smart to
	// conditionally populate hotCache.  For now just do it some
	// percentage of the time.
	if rand.Intn(10) == 0 {
		g.populateCache(key, value, &g.hotCache)
	}
	return value, nil
}

func (g *Group) lookupCache(key string) (value ByteView, ok bool) {
	if g.cacheBytes <= 0 {
		return
	}
	value, ok = g.mainCache.get(key)
	if ok {
		return
	}
	value, ok = g.hotCache.get(key)
	return
}

// 将key和value加入到缓存中
func (g *Group) populateCache(key string, value ByteView, cache *cache) {
	if g.cacheBytes <= 0 {
		return
	}
	// 加入到cache中
	cache.add(key, value)

	// Evict items from cache(s) if necessary.
	for {
		mainBytes := g.mainCache.bytes()
		hotBytes := g.hotCache.bytes()
		// mainBytes和hotBytes没有超过最大值限制，不做清理工作
		if mainBytes+hotBytes <= g.cacheBytes {
			return
		}

		// TODO(bradfitz): this is good-enough-for-now logic.
		// It should be something based on measurements and/or
		// respecting the costs of different resources.
		victim := &g.mainCache
		// hotBytes 大于 mainCache/8时，就清理hotCache，否则清理mainCache
		if hotBytes > mainBytes/8 {
			victim = &g.hotCache
		}
		// 清除最久没有使用的
		victim.removeOldest()
	}
}

// CacheType 缓存的类型
type CacheType int

const (
	// The MainCache 是当前peer缓存的items
	MainCache CacheType = iota + 1

	// The HotCache 是频繁使用的items，它可以是从其他node中复制过来的
	HotCache
)

// CacheStats returns stats about the provided cache within the group.
func (g *Group) CacheStats(which CacheType) CacheStats {
	switch which {
	case MainCache:
		return g.mainCache.stats()
	case HotCache:
		return g.hotCache.stats()
	default:
		return CacheStats{}
	}
}

type cache struct {
	mu         sync.RWMutex
	nbytes     int64 // 所有key和value的大小
	lru        *lru.Cache
	nhit, nget int64 //命中数、get请求数
	nevict     int64 // 清除的item的数量
}

// cache的统计信息：所有key和value的大小、是否锁定、get请求的数量、命中的数量、清除item的数量
func (c *cache) stats() CacheStats {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return CacheStats{
		Bytes:     c.nbytes,
		Items:     c.itemsLocked(),
		Gets:      c.nget,
		Hits:      c.nhit,
		Evictions: c.nevict,
	}
}

// 将key和value加入缓存
func (c *cache) add(key string, value ByteView) {
	c.mu.Lock()
	defer c.mu.Unlock()
	// 当lru为空的时候，就new一个lru，并定义lru满的时候的清除数据的回调方法
	if c.lru == nil {
		c.lru = &lru.Cache{
			OnEvicted: func(key lru.Key, value interface{}) {
				val := value.(ByteView)
				c.nbytes -= int64(len(key.(string))) + int64(val.Len())
				c.nevict++
			},
		}
	}
	c.lru.Add(key, value)
	// 更新key和value的大小
	c.nbytes += int64(len(key)) + int64(value.Len())
}

// 通过key从lru中获取item
func (c *cache) get(key string) (value ByteView, ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.nget++
	if c.lru == nil {
		return
	}
	vi, ok := c.lru.Get(key)
	if !ok {
		return
	}
	// 命中
	c.nhit++
	return vi.(ByteView), true
}

// 从lru中移除最久没有使用的数据
func (c *cache) removeOldest() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.lru != nil {
		c.lru.RemoveOldest()
	}
}

// 返回所有key和value的和
func (c *cache) bytes() int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.nbytes
}

// 返回lru的长度
func (c *cache) items() int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.itemsLocked()
}

func (c *cache) itemsLocked() int64 {
	if c.lru == nil {
		return 0
	}
	return int64(c.lru.Len())
}

// An AtomicInt is an int64 to 原子操作
type AtomicInt int64

// Add atomically adds n to i.
func (i *AtomicInt) Add(n int64) {
	atomic.AddInt64((*int64)(i), n)
}

// Get atomically gets the value of i.
func (i *AtomicInt) Get() int64 {
	return atomic.LoadInt64((*int64)(i))
}

func (i *AtomicInt) String() string {
	return strconv.FormatInt(i.Get(), 10)
}

// CacheStats 统计信息，are returned by stats accessors on Group.
type CacheStats struct {
	Bytes     int64
	Items     int64
	Gets      int64
	Hits      int64
	Evictions int64
}
