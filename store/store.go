package store

import (
	"fmt"
	"os"

	"github.com/cespare/xxhash/v2"
)

const fileCount = 1 //must be more then zero
const fileMode = 0666
const sizeHead = 8

var s Store

type Store struct {
	buckets [fileCount]*os.File
}

func init() {
	s = Store{}
	for idx := 0; idx < fileCount; idx++ {
		f, err := os.OpenFile(fmt.Sprintf("%d", idx), os.O_CREATE|os.O_RDWR, os.FileMode(fileMode))
		if err != nil {
			fmt.Println(err)
			os.Exit(0)
		}
		s.buckets[idx] = f
	}
}

// Write to file
// [0     1                 0 5      0 0 0 2   104 101 108 108 111 103 111]
// [flag bucketsize(2^1) keysize(5) valsize(2)  h   e   l   l   o   g   o]
func Write(k, v []byte) {
	h := xxhash.Sum64(k)
	idx := h % fileCount
	fmt.Println(h, idx)
	// write head
	vp, vs := NextPowerOf2(uint32(len(v)))
	size := sizeHead + int(vs) + len(k)
	b := make([]byte, size)
	b[0] = byte(0)
	b[1] = vp
	//len key in bigendian format
	lenk := uint16(len(k))
	b[2] = byte(lenk >> 8)
	b[3] = byte(lenk)
	//len val in bigendian format
	lenv := uint32(len(v))
	b[4] = byte(lenv >> 24)
	b[5] = byte(lenv >> 16)
	b[6] = byte(lenv >> 8)
	b[7] = byte(lenv)
	// write body
	copy(b[sizeHead:], k)
	copy(b[sizeHead+lenk:], v)
	n, _ := s.buckets[idx].Write(b)
	//b = append(b, v...)
	fmt.Printf("%+v %d %d\n", b, n, vs)
}

func Read(k []byte) {

}

// https://github.com/thejerf/gomempool/blob/master/pool.go#L519
// http://graphics.stanford.edu/~seander/bithacks.html#RoundUpPowerOf2
// suitably modified to work on 32-bit
func nextPowerOf2(v uint32) uint32 {
	v--
	v |= v >> 1
	v |= v >> 2
	v |= v >> 4
	v |= v >> 8
	v |= v >> 16
	v++

	return v
}

// NextPowerOf2 return next power of 2 for v and it's value
// return maxuint32 in case of overflow
func NextPowerOf2(v uint32) (power byte, val uint32) {
	if v > 0 {
		val = 1
	}
	for power = 0; power < 32; power++ {
		val = nextPowerOf2(val)
		if val >= v {
			break
		}
		val++
	}
	if power == 32 {
		//overflow
		val = 4294967295
	}
	return
}
