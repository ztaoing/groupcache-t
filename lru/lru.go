/*
Copyright 2013 Google Inc.

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

// Package lru 实现了一个lru缓存
package lru

import "container/list"

// 非并发安全的cache
type Cache struct {
	// cache中最大的数量,0代表没有限制
	MaxEntries int

	// 在清除元素的时候使用的回调方法
	OnEvicted func(key Key, value interface{})
	// 双向链表
	ll *list.List
	// 在map中存储映射关系
	cache map[interface{}]*list.Element
}

// A Key may be any value that is comparable. See http://golang.org/ref/spec#Comparison_operators
// key可以是任何可以比较的值
type Key interface{}

// 每一条记录
type entry struct {
	key   Key
	value interface{}
}

// 根据max初始化一个cache
func New(maxEntries int) *Cache {
	return &Cache{
		MaxEntries: maxEntries,
		ll:         list.New(),
		cache:      make(map[interface{}]*list.Element),
	}
}

// Add adds a value to the cache.
func (c *Cache) Add(key Key, value interface{}) {
	if c.cache == nil {
		// lazy
		c.cache = make(map[interface{}]*list.Element)
		c.ll = list.New()
	}
	// 如果key已经存在就更新value
	if ee, ok := c.cache[key]; ok {
		//被访问，将元素移动到链表头部
		c.ll.MoveToFront(ee)
		ee.Value.(*entry).value = value
		return
	}
	// 不存在
	// 加入到链表头部
	ele := c.ll.PushFront(&entry{key, value})
	// 保存key与value的映射关系
	c.cache[key] = ele
	// 超过最大的允许长度时，清除最久没有使用的
	if c.MaxEntries != 0 && c.ll.Len() > c.MaxEntries {
		c.RemoveOldest()
	}
}

// Get looks up a key's value from the cache.
func (c *Cache) Get(key Key) (value interface{}, ok bool) {
	if c.cache == nil {
		return
	}
	// 被访问，移动到链表的头部
	if ele, hit := c.cache[key]; hit {
		c.ll.MoveToFront(ele)
		return ele.Value.(*entry).value, true
	}
	return
}

// Remove removes the provided key from the cache.
func (c *Cache) Remove(key Key) {
	if c.cache == nil {
		return
	}
	if ele, hit := c.cache[key]; hit {
		c.removeElement(ele)
	}
}

// RemoveOldest removes the oldest item from the cache.
func (c *Cache) RemoveOldest() {
	if c.cache == nil {
		return
	}
	// 获取链表尾部元素
	ele := c.ll.Back()
	if ele != nil {
		c.removeElement(ele)
	}
}

// 移除元素
func (c *Cache) removeElement(e *list.Element) {
	// 从链表中移除
	c.ll.Remove(e)
	kv := e.Value.(*entry)
	// 从cache中移除
	delete(c.cache, kv.key)
	if c.OnEvicted != nil {
		// 使用回调处理被移除的元素
		c.OnEvicted(kv.key, kv.value)
	}
}

// Len returns the number of items in the cache.
func (c *Cache) Len() int {
	if c.cache == nil {
		return 0
	}
	return c.ll.Len()
}

// 清除cache中的所有元素
func (c *Cache) Clear() {
	if c.OnEvicted != nil {
		for _, e := range c.cache {
			kv := e.Value.(*entry)
			c.OnEvicted(kv.key, kv.value)
		}
	}
	c.ll = nil
	c.cache = nil
}
