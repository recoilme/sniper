package store

import (
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"sync"

	"github.com/cespare/xxhash/v2"
	"github.com/recoilme/sniper/file"
)

const fileCount = 1 //must be more then zero
const fileMode = 0666
const dirMode = 0777
const sizeHead = 8

// Store struct
type Store struct {
	sync.RWMutex
	dir string
	f   *os.File
	m   map[uint64]uint32
	h   map[uint32]uint8
}

//Open return new store
func Open(dir string) (s *Store, err error) {
	s = &Store{}
	s.Lock()
	defer s.Unlock()

	if dir == "" {
		dir = "."
	}

	// create dirs
	_, err = os.Stat(dir)

	if err != nil {
		// file not exists - create dirs if any
		if os.IsNotExist(err) {
			if dir != "." {
				err = os.MkdirAll(dir, os.FileMode(dirMode))
				if err != nil {
					return
				}
			}
		} else {
			return
		}
	}
	// file (0 right now)
	f, err := os.OpenFile(fmt.Sprintf("%s/%d", dir, 0), os.O_CREATE|os.O_RDWR, os.FileMode(fileMode))
	if err != nil {
		return
	}
	s.dir = dir
	s.f = f
	s.m = make(map[uint64]uint32)
	s.h = make(map[uint32]uint8)

	//read if f not empty
	if fi, e := f.Stat(); e == nil {
		if fi.Size() == 0 {
			return
		}
		//read file
		var seek int
		for {
			b := make([]byte, 8)
			n, errRead := f.Read(b)
			if errRead != nil || n != 8 {
				if errRead != nil && errRead.Error() != "EOF" {
					err = errRead
				}
				return
			}
			//readed header
			lenk := binary.BigEndian.Uint16(b[2:4])
			key := make([]byte, lenk)
			f.Read(key)
			shiftv := 1 << byte(b[0]) //2^pow
			ret, _ := f.Seek(int64(shiftv-int(lenk)), 1)
			h := xxhash.Sum64(key)
			if b[1] != 255 {
				// not deleted
				s.m[h] = uint32(seek)
			} else {
				//deleted blocks store
				s.h[uint32(seek)] = b[0]
			}
			log.Println(seek, key, ret)
			seek += int(ret)
		}
	}
	return
}

// Set Write to file
// [0     1                 0 5      0 0 0 2   104 101 108 108 111 103 111]
// [flag bucketsize(2^1) keysize(5) valsize(2)  h   e   l   l   o   g   o]
func (s *Store) Set(k, v []byte) (err error) {
	s.Lock()
	defer s.Unlock()
	h := xxhash.Sum64(k)

	// write head
	vp, vs := NextPowerOf2(uint32(len(v) + len(k)))
	size := sizeHead + int(vs)
	b := make([]byte, size)
	b[0] = vp
	b[1] = byte(0)
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

	// write at file
	pos := int64(-1)
	if seek, ok := s.m[h]; ok {
		//fmt.Println("seek", seek)
		sizeold := make([]byte, 1)
		_, err = file.ReadAtPos(s.f, sizeold, int64(seek))
		if err == nil && sizeold[0] >= vp {
			//overwrite
			pos = int64(seek)
		} else {
			// mark old k/v as deleted
			delb := make([]byte, 1)
			delb[0] = 255
			file.WriteAtPos(s.f, delb, int64(seek+1))
			// add addr 2 holes
			s.h[seek] = sizeold[0]
			//log.Println("mark old k/v as deleted")
			// try to find optimal empty hole
			for addr, size := range s.h {
				if size == vp {
					pos = int64(addr)
					delete(s.h, addr)
					break
				}
			}
		}
	}
	// write at end or in hole or overwrite
	seek, _, err := file.WriteAtPos(s.f, b, pos)
	s.m[h] = uint32(seek)
	//fmt.Printf("%+v %d %d %d\n", b, n, vs, seek)
	return
}

// Get return val by key
func (s *Store) Get(k []byte) (v []byte, err error) {
	s.RLock()
	defer s.RUnlock()
	h := xxhash.Sum64(k)
	if seek, ok := s.m[h]; ok {
		b := make([]byte, 8)
		s.f.ReadAt(b, int64(seek))
		lenk := binary.BigEndian.Uint16(b[2:4])
		lenv := binary.BigEndian.Uint32(b[4:8])
		v = make([]byte, lenv)
		_, err = s.f.ReadAt(v, int64(seek+8+uint32(lenk)))
	}
	return
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
