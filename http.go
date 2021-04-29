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

package groupcache_t

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"

	"github.com/golang/groupcache/consistenthash"
	pb "github.com/golang/groupcache/groupcachepb"
	"github.com/golang/protobuf/proto"
)

const defaultBasePath = "/_groupcache/"

const defaultReplicas = 50

// HTTPPool implements PeerPicker for a pool of HTTP peers.
// HTTPPool是实现了PeerPicker接口的一个HTTP连接池
type HTTPPool struct {
	// Context是一个可选的配置，他是当收到request的时候，供server使用的，如果没有设置，就使用request的中的context
	Context func(*http.Request) context.Context

	// 当创建一个request的时候，可以指定一个http.RoundTripper供client使用。如果没有设置，就使用默认的http.DefaultTransport
	Transport func(context.Context) http.RoundTripper

	// 当前 peer的 base URL, e.g. "https://example.net:8000"
	self string

	// opts 定义了HTTPpool的配置项:包括了处理请求的地址、key在一致性hash中的副本数量和生成hash值的方法
	opts HTTPPoolOptions

	mu          sync.Mutex // 保护 peers 和 httpGetters
	peers       *consistenthash.Map
	httpGetters map[string]*httpGetter // keyed by e.g. "http://10.0.0.2:8008"
}

// HTTPPoolOptions 是HTTPPool的配置项.
type HTTPPoolOptions struct {
	// BasePath 指定了处理groupcache的请求的 HTTP 地址
	// 如果没有设置，就使用默认值: "/_groupcache/".
	BasePath string

	// 指定了在一致性hash中key的副本数量，如果没有设置，就使用默认值50
	Replicas int

	// 用于生成一致性hash值的方法，如果没有设置，就使用crc32.ChecksumIEEE.
	HashFn consistenthash.Hash
}

// NewHTTPPool initializes an HTTP pool of peers, and registers itself as a PeerPicker.
// For convenience, it also registers itself as an http.Handler with http.DefaultServeMux.
// The self argument should be a valid base URL that points to the current server,
// for example "http://example.net:8000".
func NewHTTPPool(self string) *HTTPPool {
	p := NewHTTPPoolOpts(self, nil)
	http.Handle(p.opts.BasePath, p)
	return p
}

var httpPoolMade bool

// NewHTTPPoolOpts initializes an HTTP pool of peers with the given options.
// Unlike NewHTTPPool, this function does not register the created pool as an HTTP handler.
// The returned *HTTPPool implements http.Handler and must be registered using http.Handle.
func NewHTTPPoolOpts(self string, o *HTTPPoolOptions) *HTTPPool {
	if httpPoolMade {
		panic("groupcache: NewHTTPPool must be called only once")
	}
	httpPoolMade = true

	p := &HTTPPool{
		self:        self,
		httpGetters: make(map[string]*httpGetter),
	}
	if o != nil {
		p.opts = *o
	}
	if p.opts.BasePath == "" {
		p.opts.BasePath = defaultBasePath
	}
	if p.opts.Replicas == 0 {
		p.opts.Replicas = defaultReplicas
	}
	p.peers = consistenthash.New(p.opts.Replicas, p.opts.HashFn)
	// 注册
	RegisterPeerPicker(func() PeerPicker { return p })
	return p
}

// 更新pool中peers列表,每一个peer必须是一个合法的URL for example "http://example.net:8000".
func (p *HTTPPool) Set(peers ...string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	// 创建一个新map
	p.peers = consistenthash.New(p.opts.Replicas, p.opts.HashFn)
	// 将peers添加到map中
	p.peers.Add(peers...)

	p.httpGetters = make(map[string]*httpGetter, len(peers))
	// 存储映射关系
	for _, peer := range peers {
		p.httpGetters[peer] = &httpGetter{transport: p.Transport, baseURL: peer + p.opts.BasePath}
	}
}

func (p *HTTPPool) PickPeer(key string) (ProtoGetter, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.peers.IsEmpty() {
		return nil, false
	}
	// 通过key获取peer，并且获取到的peer不是本地的peer时，从httpGetters中获取httpGetter
	if peer := p.peers.Get(key); peer != p.self {
		return p.httpGetters[peer], true
	}
	return nil, false
}

func (p *HTTPPool) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// 解析请求:如果请求中的path 不等于basepath就panic
	if !strings.HasPrefix(r.URL.Path, p.opts.BasePath) {
		panic("HTTPPool serving unexpected path: " + r.URL.Path)
	}
	// 获取basepath之后的参数,最多切分出2部分
	parts := strings.SplitN(r.URL.Path[len(p.opts.BasePath):], "/", 2)
	if len(parts) != 2 {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	groupName := parts[0]
	key := parts[1]

	// 通过groupName找到group
	group := GetGroup(groupName)
	if group == nil {
		http.Error(w, "no such group: "+groupName, http.StatusNotFound)
		return
	}
	var ctx context.Context
	if p.Context != nil {
		// 使用context处理request中context
		ctx = p.Context(r)
	} else {
		// 直接使用request中context
		ctx = r.Context()
	}

	group.Stats.ServerRequests.Add(1)

	var value []byte
	// todo
	err := group.Get(ctx, key, AllocatingByteSliceSink(&value))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// 将value写入到response的body中，作为一条protocol信息
	body, err := proto.Marshal(&pb.GetResponse{Value: value})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/x-protobuf")
	w.Write(body)
}

type httpGetter struct {
	transport func(context.Context) http.RoundTripper
	baseURL   string
}

// 使用sync.pool来创建资源，并设置了new方法，当没有资源时，会通过new定义的方法创建一个供调用者使用
var bufferPool = sync.Pool{
	New: func() interface{} { return new(bytes.Buffer) },
}

func (h *httpGetter) Get(ctx context.Context, in *pb.GetRequest, out *pb.GetResponse) error {
	u := fmt.Sprintf(
		"%v%v/%v",
		h.baseURL,
		url.QueryEscape(in.GetGroup()),
		url.QueryEscape(in.GetKey()),
	)

	req, err := http.NewRequest("GET", u, nil)
	if err != nil {
		return err
	}
	req = req.WithContext(ctx)

	tr := http.DefaultTransport
	if h.transport != nil {
		tr = h.transport(ctx)
	}

	res, err := tr.RoundTrip(req)

	if err != nil {
		return err
	}

	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("server returned: %v", res.Status)
	}
	// 从buffer pool中获取一个
	b := bufferPool.Get().(*bytes.Buffer)
	b.Reset()
	// 在用完后归还到pool中
	defer bufferPool.Put(b)

	_, err = io.Copy(b, res.Body)
	if err != nil {
		return fmt.Errorf("reading response body: %v", err)
	}
	err = proto.Unmarshal(b.Bytes(), out)
	if err != nil {
		return fmt.Errorf("decoding response body: %v", err)
	}
	return nil
}
