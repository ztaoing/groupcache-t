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

// Package singleflight 提供了重复调用函数时的抑制机制
package singleflight

import "sync"

// call 是一个执行中或者完成了的调用
type call struct {
	wg  sync.WaitGroup
	val interface{}
	err error
}

// Group represents a class of work and forms a namespace in which
// units of work can be executed with duplicate suppression.
// group使用互斥锁的方式对map进行保护，

// Group是flightGroup接口的一个实现
type Group struct {
	mu sync.Mutex       // protects m
	m  map[string]*call // lazily initialized
}

// Do 执行并且返回了给定函数的执行结果，确保对于一个key在同一时间，只有操作可以执行
// 如果重复的请求到来，那么这个重复的调用会等待之前的函数的完成，并使用它的结果，作为自己的结果
func (g *Group) Do(key string, fn func() (interface{}, error)) (interface{}, error) {
	g.mu.Lock()
	if g.m == nil {
		// lazy
		g.m = make(map[string]*call)
	}
	// 如果需要执行的key已经存在，就加入等待队列,等待执行的结果
	if c, ok := g.m[key]; ok {
		g.mu.Unlock()
		c.wg.Wait()
		return c.val, c.err
	}
	//如果key不存在，就创建一个调用
	c := new(call)

	// 锁，设置key
	c.wg.Add(1)
	g.m[key] = c
	g.mu.Unlock()

	// 执行处理函数
	c.val, c.err = fn()
	c.wg.Done()

	//解除对key的锁定
	g.mu.Lock()
	delete(g.m, key)
	g.mu.Unlock()

	return c.val, c.err
}
