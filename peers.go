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

// peers.go defines how processes find and communicate with their peers.

package groupcache_t

import (
	"context"

	pb "github.com/golang/groupcache/groupcachepb"
)

// Context 是 context.Context的别名，用于向后兼容
type Context = context.Context

// ProtoGetter 是一个接口，peer必须实现这个接口
type ProtoGetter interface {
	Get(ctx context.Context, in *pb.GetRequest, out *pb.GetResponse) error
}

// PeerPicker 是一个接口，它必须被本地的拥有指定key的peer实现
//is the interface that must be implemented to locate the peer that owns a specific key.
type PeerPicker interface {
	// PickPeer 返回通过key找到的peer，returns the peer that owns the specific key
	// and返回true的话表明有一个远端的peer被找到， true to indicate that a remote peer was nominated.
	// 如果通过key找到的peer是本地peer的话，就返回nil和false
	PickPeer(key string) (peer ProtoGetter, ok bool)
}

// 是PeerPicker的一个实现，并永远不会找到任何一个peer
type NoPeers struct{}

// 直接返回默认值
func (NoPeers) PickPeer(key string) (peer ProtoGetter, ok bool) { return }

var (
	portPicker func(groupName string) PeerPicker
)

// RegisterPeerPicker 注册一个peer初始化的方法
// 当第一个group被创建的时候，它就会被调用一次
// RegisterPeerPicker和RegisterPerGroupPeerPicker都只能被调用一次
func RegisterPeerPicker(fn func() PeerPicker) {
	if portPicker != nil {
		// 只能调用一次
		panic("RegisterPeerPicker called more than once")
	}
	portPicker = func(_ string) PeerPicker { return fn() }
}

// RegisterPerGroupPeerPicker registers the peer initialization function,
// which takes the groupName, to be used in choosing a PeerPicker.
// It is called once, when the first group is created.
// Either RegisterPeerPicker or RegisterPerGroupPeerPicker should be
// called exactly once, but not both.
func RegisterPerGroupPeerPicker(fn func(groupName string) PeerPicker) {
	if portPicker != nil {
		panic("RegisterPeerPicker called more than once")
	}
	portPicker = fn
}

// 通过groupName查找PeerPicker
func getPeers(groupName string) PeerPicker {
	if portPicker == nil {
		return NoPeers{}
	}
	pk := portPicker(groupName)
	if pk == nil {
		pk = NoPeers{}
	}
	return pk
}
