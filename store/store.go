package store

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"sync"

	"github.com/cespare/xxhash/v2"
)

const fileCount = 1 //must be more then zero
const fileMode = 0666
const dirMode = 0777
const sizeHead = 8
const deleted = 42 // tribute 2 dbf, lol

var errFormat = errors.New("Unexpected file format")
var errNotFound = errors.New("Error key not found")

// Store struct
// f - file with data
// m - keys: hash / addr
// h - holes: addr / size
type Store struct {
	sync.RWMutex
	dir string
	f   *os.File          // file storage
	m   map[uint64]uint32 //keys: hash / addr
	h   map[uint32]uint8  //holes: addr / size
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

	// file (0 hardcoded right now)
	f, err := os.OpenFile(fmt.Sprintf("%s/%d", dir, 0), os.O_CREATE|os.O_RDWR, os.FileMode(fileMode))
	if err != nil {
		return
	}
	s.dir = dir
	s.f = f
	s.m = make(map[uint64]uint32)
	s.h = make(map[uint32]uint8)

	//read if f not empty
	if fi, e := s.f.Stat(); e == nil {
		if fi.Size() == 0 {
			return
		}
		//read file
		//log.Println("Open", fi.Size())
		var seek int
		for {
			b := make([]byte, 8)
			n, errRead := s.f.Read(b)
			if errRead != nil || n != 8 {
				if errRead != nil && errRead.Error() != "EOF" {
					err = errRead
				}
				break
			}
			//readed header
			lenk := binary.BigEndian.Uint16(b[2:4])
			if lenk == 0 {
				return nil, errFormat
			}
			key := make([]byte, lenk)
			n, errRead = s.f.Read(key)
			if errRead != nil || n != int(lenk) {
				return nil, errFormat
			}
			shiftv := 1 << byte(b[0])                            //2^pow
			ret, seekerr := s.f.Seek(int64(shiftv-int(lenk)), 1) // skip val && key
			if seekerr != nil {
				return nil, errFormat
			}
			// map store
			if b[1] != deleted {
				h := xxhash.Sum64(key)
				s.m[h] = uint32(seek)
			} else {
				//deleted blocks store
				s.h[uint32(seek)] = b[0] // seek / size
			}
			seek = int(ret)
		}
	}
	return
}

// Set Write to file
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
		_, err := s.f.ReadAt(sizeold, int64(seek))
		if err != nil {
			return err
		}
		if err == nil && sizeold[0] == vp {
			//overwrite
			pos = int64(seek)
		} else {
			// mark old k/v as deleted
			delb := make([]byte, 1)
			delb[0] = deleted
			s.f.WriteAt(delb, int64(seek+1))

			s.h[seek] = sizeold[0]
			//log.Println("hole", int64(seek), sizeold[0])

			// try to find optimal empty hole
			for addrh, sizeh := range s.h {
				if sizeh == vp {
					pos = int64(addrh)
					//log.Println("find hole", addrh, sizeh)
					delete(s.h, addrh)
					break
				}
			}
		}
	}
	// write at end or in hole or overwrite
	if pos < 0 {
		pos, err = s.f.Seek(0, 2) // append to the end of file
	}
	s.f.WriteAt(b, pos)
	s.m[h] = uint32(pos)
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
		_, err = s.f.ReadAt(v, int64(seek+sizeHead+uint32(lenk)))
	} else {
		return nil, errNotFound
	}
	return
}

// Count return count keys
func (s *Store) Count() int {
	return len(s.m)
}

// Close - close related file
func (s *Store) Close() error {
	return s.f.Close()
}

// DeleteFile - delete file
func (s *Store) DeleteFile() error {
	return os.Remove(s.f.Name())
}

// DeleteStore - remove directory with files
func DeleteStore(dir string) error {
	return os.RemoveAll(dir)
}

// FileSize returns the total size of the disk storage used by the DB.
func (s *Store) FileSize() (int64, error) {
	var err error
	is, err := s.f.Stat()
	if err != nil {
		return -1, err
	}
	return is.Size(), nil
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
