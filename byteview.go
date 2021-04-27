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

package groupcache_t

import (
	"bytes"
	"errors"
	"io"
	"strings"
)

// 一个 ByteView 持有对bytes的持久不变的视图
// 它的内部包含了一个 []byte 和一个string，但是调用者是无法获知的(非导出的)

// A ByteView 是被用来作为一个value类型的，而不是一个指针类型 (例如 time.Time).
type ByteView struct {
	// 如果b是非空的, 使用b或者使用s
	b []byte
	s string
}

// 返回视图的长度
func (v ByteView) Len() int {
	if v.b != nil {
		return len(v.b)
	}
	return len(v.s)
}

// ByteSlice returns 将数据拷贝一份并作为一个[]byte
func (v ByteView) ByteSlice() []byte {
	if v.b != nil {
		return cloneBytes(v.b)
	}
	return []byte(v.s)
}

// String 将数转换为string，并返回, 如果需要的话就制作一个副本
func (v ByteView) String() string {
	if v.b != nil {
		return string(v.b)
	}
	return v.s
}

// 返回[]byte中第i个元素
func (v ByteView) At(i int) byte {
	if v.b != nil {
		return v.b[i]
	}
	return v.s[i]
}

// Slice slices the view between the provided from and to indices.
func (v ByteView) Slice(from, to int) ByteView {
	if v.b != nil {
		return ByteView{b: v.b[from:to]}
	}
	return ByteView{s: v.s[from:to]}
}

// SliceFrom slices the view from the provided index until the end.
func (v ByteView) SliceFrom(from int) ByteView {
	if v.b != nil {
		return ByteView{b: v.b[from:]}
	}
	return ByteView{s: v.s[from:]}
}

// Copy copies b into dest and returns the number of bytes copied.
// 返回copy的数量
func (v ByteView) Copy(dest []byte) int {
	if v.b != nil {
		return copy(dest, v.b)
	}
	return copy(dest, v.s)
}

// Equal returns whether the bytes in b are the same as the bytes in
// b2.
func (v ByteView) Equal(b2 ByteView) bool {
	if b2.b == nil {
		return v.EqualString(b2.s)
	}
	return v.EqualBytes(b2.b)
}

// EqualString returns whether the bytes in b are the same as the bytes
// in s.
func (v ByteView) EqualString(s string) bool {
	if v.b == nil {
		return v.s == s
	}
	l := v.Len()
	if len(s) != l {
		return false
	}
	// 比较每一个字符
	for i, bi := range v.b {
		if bi != s[i] {
			return false
		}
	}
	return true
}

// EqualBytes returns whether the bytes in b are the same as the bytes
// in b2.
func (v ByteView) EqualBytes(b2 []byte) bool {
	if v.b != nil {
		return bytes.Equal(v.b, b2)
	}
	l := v.Len()
	if len(b2) != l {
		return false
	}
	for i, bi := range b2 {
		if bi != v.s[i] {
			return false
		}
	}
	return true
}

// Reader returns an io.ReadSeeker for the bytes in v.
func (v ByteView) Reader() io.ReadSeeker {
	if v.b != nil {
		return bytes.NewReader(v.b)
	}
	return strings.NewReader(v.s)
}

// ReadAt implements io.ReaderAt on the bytes in v.
func (v ByteView) ReadAt(p []byte, off int64) (n int, err error) {
	if off < 0 {
		return 0, errors.New("view: invalid offset")
	}
	if off >= int64(v.Len()) {
		return 0, io.EOF
	}
	n = v.SliceFrom(int(off)).Copy(p)
	if n < len(p) {
		err = io.EOF
	}
	return
}

// WriteTo implements io.WriterTo on the bytes in v.
func (v ByteView) WriteTo(w io.Writer) (n int64, err error) {
	var m int
	if v.b != nil {
		m, err = w.Write(v.b)
	} else {
		m, err = io.WriteString(w, v.s)
	}
	if err == nil && m < v.Len() {
		err = io.ErrShortWrite
	}
	n = int64(m)
	return
}
