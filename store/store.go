package store

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"sync"

	"github.com/cespare/xxhash/v2"
)

const chunksCount = 16 //must be more then zero
const fileMode = 0666
const dirMode = 0777
const sizeHead = 8
const deleted = 42 // flag for removed, tribute 2 dbf!

var errFormat = errors.New("Unexpected file format")
var errNotFound = errors.New("Error key not found")
var counters sync.Map

// Store struct
// data sharded by chunks
type Store struct {
	chunks [chunksCount]chunk
	dir    string
}

// chunk
type chunk struct {
	sync.RWMutex
	f *os.File          // file storage
	m map[uint64]uint64 // keys: hash / addr&len
	h map[uint32]uint8  // holes: addr / size
}

func (c *chunk) Init(name string) (err error) {
	c.Lock()
	defer c.Unlock()

	f, err := os.OpenFile(name, os.O_CREATE|os.O_RDWR, os.FileMode(fileMode))
	if err != nil {
		return
	}
	err = f.Sync()
	if err != nil {
		return
	}
	c.f = f
	c.m = make(map[uint64]uint64)
	c.h = make(map[uint32]uint8)

	//read if f not empty
	if fi, e := c.f.Stat(); e == nil {
		if fi.Size() == 0 {
			return
		}
		//read file
		var seek int
		for {
			b := make([]byte, 8)
			n, errRead := c.f.Read(b)
			if errRead != nil || n != 8 {
				if errRead != nil && errRead.Error() != "EOF" {
					err = errRead
				}
				break
			}
			//readed header
			lenk := binary.BigEndian.Uint16(b[2:4])
			lenv := binary.BigEndian.Uint32(b[4:8])
			if lenk == 0 {
				return errFormat
			}
			// skip val
			_, seekerr := c.f.Seek(int64(lenv), 1)
			if seekerr != nil {
				return errFormat
			}
			// read key
			key := make([]byte, lenk)
			n, errRead = c.f.Read(key)
			if errRead != nil || n != int(lenk) {
				return errFormat
			}
			shiftv := 1 << byte(b[0])                                      //2^pow
			ret, seekerr := c.f.Seek(int64(shiftv-int(lenk)-int(lenv)), 1) // skip val && key
			if seekerr != nil {
				return errFormat
			}
			// map store
			if b[1] != deleted {
				h := xxhash.Sum64(key)
				c.m[h] = seeklenPack(uint32(seek), lenv)
			} else {
				//deleted blocks store
				c.h[uint32(seek)] = b[0] // seek / size
			}
			seek = int(ret)
		}
	}
	return
}

// Open return new store
// dir will be created
func Open(dir string) (s *Store, err error) {
	s = &Store{}

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

	// create chuncks
	for i := range s.chunks[:] {
		err = s.chunks[i].Init(fmt.Sprintf("%s/%d", dir, i))
		if err != nil {
			return
		}
	}
	return
}

// pack addr & len of value 2 uint64
func seeklenPack(seek, lenv uint32) uint64 {
	p := make([]byte, 8)
	p[0] = byte(seek >> 24)
	p[1] = byte(seek >> 16)
	p[2] = byte(seek >> 8)
	p[3] = byte(seek)
	p[4] = byte(lenv >> 24)
	p[5] = byte(lenv >> 16)
	p[6] = byte(lenv >> 8)
	p[7] = byte(lenv)
	return binary.BigEndian.Uint64(p)
}

// unpack addr & len
func seeklenUnpack(seeklen uint64) (seek, lenv uint32) {
	p := make([]byte, 8)
	binary.BigEndian.PutUint64(p, seeklen)
	return binary.BigEndian.Uint32(p[:4]), binary.BigEndian.Uint32(p[4:8])
}

// Set - calc chunk idx and write in it
func (s *Store) Set(k, v []byte) (err error) {
	h := xxhash.Sum64(k)
	idx := h % chunksCount
	return s.chunks[idx].set(k, v, h)
}

// set - write data to file & in map
func (c *chunk) set(k, v []byte, h uint64) (err error) {
	c.Lock()
	defer c.Unlock()

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
	// write body: val and key
	copy(b[sizeHead:], v)
	copy(b[sizeHead+lenv:], k)

	// write at file
	pos := int64(-1)

	if seeklen, ok := c.m[h]; ok {
		seek, _ := seeklenUnpack(seeklen)
		sizeold := make([]byte, 1)
		_, err := c.f.ReadAt(sizeold, int64(seek))
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
			c.f.WriteAt(delb, int64(seek+1))

			c.h[seek] = sizeold[0]

			// try to find optimal empty hole
			for addrh, sizeh := range c.h {
				if sizeh == vp {
					pos = int64(addrh)
					delete(c.h, addrh)
					break
				}
			}
		}
	}
	// write at end or in hole or overwrite
	if pos < 0 {
		pos, err = c.f.Seek(0, 2) // append to the end of file
	}
	c.f.WriteAt(b, pos)
	c.m[h] = seeklenPack(uint32(pos), lenv)
	return
}

// Get - calc chunk idx and get from it
func (s *Store) Get(k []byte) (v []byte, err error) {
	h := xxhash.Sum64(k)
	idx := h % chunksCount
	return s.chunks[idx].get(k, h)
}

// get return val by key
func (c *chunk) get(k []byte, h uint64) (v []byte, err error) {
	c.RLock()
	defer c.RUnlock()
	if seeklen, ok := c.m[h]; ok {
		seek, lenv := seeklenUnpack(seeklen)

		v = make([]byte, lenv)
		_, err = c.f.ReadAt(v, int64(seek+sizeHead))
	} else {
		return nil, errNotFound
	}
	return
}

// Count return count keys
func (s *Store) Count() (cnt int) {
	for i := range s.chunks[:] {
		cnt += s.chunks[i].count()
	}
	return
}

// return map length
func (c *chunk) count() int {
	c.RLock()
	defer c.RUnlock()
	return len(c.m)
}

// Close - close related chunks
func (s *Store) Close() (err error) {
	for i := range s.chunks[:] {
		err = s.chunks[i].close()
		if err != nil {
			return
		}
	}
	return
}

// close file
func (c *chunk) close() (err error) {
	c.Lock()
	defer c.Unlock()
	return c.f.Close()
}

// DeleteStore - remove directory with files
func DeleteStore(dir string) error {
	return os.RemoveAll(dir)
}

// FileSize returns the total size of the disk storage used by the DB.
func (s *Store) FileSize() (fs int64, err error) {
	for i := range s.chunks[:] {
		is, err := s.chunks[i].fileSize()
		if err != nil {
			return -1, err
		}
		fs += is
	}
	return
}

func (c *chunk) fileSize() (int64, error) {
	c.Lock()
	defer c.Unlock()
	is, err := c.f.Stat()
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

// Delete - delete item by key
func (s *Store) Delete(k []byte) (bool, error) {
	h := xxhash.Sum64(k)
	idx := h % chunksCount
	return s.chunks[idx].delete(k, h)
}

// delete mark item as deleted at specified position
func (c *chunk) delete(k []byte, h uint64) (isDeleted bool, err error) {
	c.Lock()
	defer c.Unlock()
	if seeklen, ok := c.m[h]; ok {
		seek, _ := seeklenUnpack(seeklen)

		sizeold := make([]byte, 1)
		_, err = c.f.ReadAt(sizeold, int64(seek))
		if err != nil {
			return
		}

		delb := make([]byte, 1)
		delb[0] = deleted
		_, err = c.f.WriteAt(delb, int64(seek+1))
		if err != nil {
			return
		}
		delete(c.m, h)
		c.h[seek] = sizeold[0]
		isDeleted = true
	}
	return
}
