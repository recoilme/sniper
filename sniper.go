package sniper

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/spaolacci/murmur3"
)

//max chunk size 128Mb
const chunksCnt = 16  //must be more then zero
const chunkColCnt = 4 // chunks for collisions
const fileMode = 0666
const dirMode = 0777
const sizeHead = 8
const deleted = 42 // flag for removed, tribute 2 dbf!

var errFormat = errors.New("Unexpected file format")
var errNotFound = errors.New("Error key not found")
var errCollision = errors.New("Error hash collision")
var counters sync.Map
var mutex = &sync.RWMutex{} //global mutex for counters and so on
//var hash32a = fnv.New32a()

// Store struct
// data sharded by chunks
type Store struct {
	chunks [chunksCnt]chunk
	dir    string
}

// chunk
type chunk struct {
	sync.RWMutex
	f *os.File          // file storage
	m map[uint32]uint32 // keys: hash / addr&len
	h map[uint32]byte   // holes: addr / size
}

func hash(b []byte) uint32 {
	return murmur3.Sum32WithSeed(b, 0)
	/*
			//MASK_24 := uint32((1 << 24) - 1)
		//hash32a.Reset()
		//tab := crc32.IEEE simpleMakeTable(IEEE)
		//h := fnv.New32()
		return murmur3.Sum32WithSeed(b, 0)
		//mur := murmur3.New32()
		//mur.Write(b)
		//h32 := mur.Sum32()
		//h32 := xxh.Checksum32(b)
		//h.Reset()
		//h.Write(b)
		//h32 := h.Sum32()
		//println(h32)
		//return h32

		//hash32a :=
		//hash32a.Write(b)
		//ss := h.Sum32()
		//hash := (ss >> 24) ^ (ss & MASK_24)
		//return hash32a.Sum32()
	*/
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
	c.m = make(map[uint32]uint32)
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
			shiftv := 1 << byte(b[0])                                               //2^pow
			ret, seekerr := c.f.Seek(int64(shiftv-int(lenk)-int(lenv)-sizeHead), 1) // skip val && key
			if seekerr != nil {
				return errFormat
			}
			// map store
			if b[1] != deleted {
				h := hash(key)
				c.m[h] = addrsizePack(uint32(seek), b[0])
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
	println("Opened")
	return
}

// pack addr & size of packet to uint32
func addrsizePack(addr uint32, size uint8) uint32 {
	/*p := make([]byte, 4)
	p[0] = size
	p[1] = byte(addr >> 16)
	p[2] = byte(addr >> 8)
	p[3] = byte(addr)
	return binary.BigEndian.Uint32(p)*/
	p := make([]byte, 4)
	//мы пишем в 3 бита хвостик от степени двойки
	// а 5 бит используем для хранения размера пакета в степенях двойки (2^5) максимум 2^32()
	// 7 это битовая маска
	p[0] = (size << 3) | (byte(addr>>24) & 7)
	p[1] = byte(addr >> 16)
	p[2] = byte(addr >> 8)
	p[3] = byte(addr)
	return binary.BigEndian.Uint32(p)
}

// unpack addr & size of packet
func addrsizeUnpack(addrsize uint32) (addr, size uint32) {
	p := make([]byte, 4)
	binary.BigEndian.PutUint32(p, addrsize)
	size = (1 << (p[0] >> 3))
	p[0] = p[0] & 7
	return binary.BigEndian.Uint32(p), size
	/*p := make([]byte, 4)
	binary.BigEndian.PutUint32(p, addrsize)
	size = (1 << p[0])
	p[0] = 0
	return binary.BigEndian.Uint32(p), size*/
}

func idx(h uint32) uint32 {
	return (h % (chunksCnt - chunkColCnt)) + chunkColCnt
}

func packetMarshal(k, v []byte) (sizeb byte, b []byte) {
	// write head
	sizeb, size := NextPowerOf2(uint32(len(v) + len(k) + sizeHead))
	b = make([]byte, size)
	b[0] = sizeb
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
	return
}

func packetUnmarshal(packet []byte) (k, v []byte, sizeb byte) {
	_ = packet[7]
	sizeb = packet[0]
	lenk := binary.BigEndian.Uint16(packet[2:4])
	lenv := binary.BigEndian.Uint32(packet[4:8])
	k = packet[sizeHead+lenv : sizeHead+lenv+uint32(lenk)]
	v = packet[sizeHead : sizeHead+lenv]
	return
}

// Set - calc chunk idx and write in it
func (s *Store) Set(k, v []byte) (err error) {
	h := hash(k) //xxhash.Sum64(k)
	idx := idx(h)
	err = s.chunks[idx].set(k, v, h)
	if err == errCollision {
		for i := 0; i < chunkColCnt; i++ {
			err = s.chunks[i].set(k, v, h)
			if err == errCollision {
				continue
			}
			break
		}
	}
	return
}

// set - write data to file & in map
func (c *chunk) set(k, v []byte, h uint32) (err error) {
	c.Lock()
	defer c.Unlock()
	sizeb, b := packetMarshal(k, v)
	// write at file
	pos := int64(-1)

	if addrsize, ok := c.m[h]; ok {
		addr, size := addrsizeUnpack(addrsize)
		packet := make([]byte, size)
		_, err = c.f.ReadAt(packet, int64(addr))
		if err != nil {
			return
		}
		key, _, sizeold := packetUnmarshal(packet)
		if !bytes.Equal(key, k) {
			//println(string(key), string(k))
			return errCollision
		}

		if err == nil && sizeold == sizeb {
			//overwrite
			pos = int64(addr)
		} else {
			// mark old k/v as deleted
			delb := make([]byte, 1)
			delb[0] = deleted
			c.f.WriteAt(delb, int64(addr+1))

			c.h[addr] = sizeold

			// try to find optimal empty hole
			for addrh, sizeh := range c.h {
				if sizeh == sizeb {
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
	c.m[h] = addrsizePack(uint32(pos), sizeb)
	return
}

// Get - calc chunk idx and get from it
func (s *Store) Get(k []byte) (v []byte, err error) {
	h := hash(k)
	idx := idx(h)
	v, err = s.chunks[idx].get(k, h)
	if err == errNotFound {
		println(err.Error(), string(k), idx)
	}
	if err == errCollision {
		for i := 0; i < chunkColCnt; i++ {

			v, err = s.chunks[i].get(k, h)
			if err == errNotFound {
				println(i, err.Error(), string(k), idx, i)
			}
			if err == errCollision || err == errNotFound {
				continue
			}
			break
		}
	}
	return
}

// get return val by key
func (c *chunk) get(k []byte, h uint32) (v []byte, err error) {
	c.RLock()
	defer c.RUnlock()
	if addrsize, ok := c.m[h]; ok {
		//p := make([]byte, 4)
		//binary.BigEndian.PutUint32(p, addrsize)
		//fmt.Printf("kv:%+v\n", p)
		addr, size := addrsizeUnpack(addrsize)
		//println("addr,size", addr, size, h)
		packet := make([]byte, size)
		_, err = c.f.ReadAt(packet, int64(addr))
		if err != nil {
			return
		}
		key, val, _ := packetUnmarshal(packet)
		//println("get", string(key), string(k), h)
		if !bytes.Equal(key, k) {
			return nil, errCollision
		}
		v = val
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
	h := hash(k)
	idx := idx(h)
	return s.chunks[idx].delete(k, h)
}

// delete mark item as deleted at specified position
func (c *chunk) delete(k []byte, h uint32) (isDeleted bool, err error) {
	c.Lock()
	defer c.Unlock()
	if addrsize, ok := c.m[h]; ok {
		addr, _ := addrsizeUnpack(addrsize)

		sizeold := make([]byte, 1)
		_, err = c.f.ReadAt(sizeold, int64(addr))
		if err != nil {
			return
		}

		delb := make([]byte, 1)
		delb[0] = deleted
		_, err = c.f.WriteAt(delb, int64(addr+1))
		if err != nil {
			return
		}
		delete(c.m, h)
		c.h[addr] = sizeold[0]
		isDeleted = true
	}
	return
}

// Incr - Incr item by uint64
// inited with zero
func (s *Store) Incr(k []byte, v uint64) (uint64, error) {
	h := hash(k)
	idx := idx(h)
	return s.chunks[idx].incrdecr(k, h, v, true)
}

// Decr - Decr item by uint64
// inited with zero
func (s *Store) Decr(k []byte, v uint64) (uint64, error) {
	h := hash(k)
	idx := idx(h)
	return s.chunks[idx].incrdecr(k, h, v, false)
}

func (c *chunk) incrdecr(k []byte, h uint32, v uint64, isIncr bool) (counter uint64, err error) {
	mutex.Lock()
	defer mutex.Unlock()
	old, err := c.get(k, h)
	if err == errNotFound {
		//create empty counter
		old = make([]byte, 8)
		err = nil
	}
	if len(old) != 8 {
		//better, then panic
		return 0, errors.New("Unexpected value format")
	}
	if err != nil {
		return
	}
	counter = binary.BigEndian.Uint64(old)
	if isIncr {
		counter += v
	} else {
		//decr
		counter -= v
	}
	new := make([]byte, 8)
	binary.BigEndian.PutUint64(new, counter)
	err = c.set(k, new, h)

	return
}

// Backup remove files with same name with bak extension
// and create new backup
func (s *Store) Backup() (err error) {
	for i := range s.chunks[:] {
		err = s.chunks[i].backup()
		if err != nil {
			return
		}
	}
	return
}

func (c *chunk) backup() (err error) {
	c.Lock()
	defer c.Unlock()
	name := c.f.Name() + ".bak"
	os.Remove(name)

	dest, err := os.OpenFile(name, os.O_CREATE|os.O_RDWR, os.FileMode(fileMode))
	if err != nil {
		return
	}
	err = c.f.Sync()
	if err != nil {
		return
	}
	_, err = c.f.Seek(0, 0)
	if err != nil {
		return
	}
	if _, err = io.Copy(dest, c.f); err != nil {
		return
	}
	dest.Sync()
	return dest.Close()
}

func readUint32(b []byte) uint32 {
	_ = b[3]
	return uint32(b[3]) | uint32(b[2])<<8 | uint32(b[1])<<16 | uint32(b[0])<<24
}

func appendUint32(b []byte, x uint32) []byte {
	a := [4]byte{
		byte(x >> 24),
		byte(x >> 16),
		byte(x >> 8),
		byte(x),
	}
	return append(b, a[:]...)
}
