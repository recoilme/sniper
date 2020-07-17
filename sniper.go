package sniper

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/pieterclaerhout/go-waitgroup"
	"github.com/spaolacci/murmur3"
	"github.com/tidwall/interval"
)

//max chunk size 256Mb
const chunksCnt = 256 //must be more then zero
const chunkColCnt = 4 // chunks for collisions
const fileMode = 0666
const dirMode = 0777
const sizeHead = 8
const deleted = 42 // flag for removed, tribute 2 dbf!
//ErrCollision -  must not happen
var ErrCollision = errors.New("Error, hash collision")

var ErrFormat = errors.New("Error, unexpected file format")
var ErrNotFound = errors.New("Error, key not found")

var ErrAddrSize = errors.New("Error, unexpected address/size")
var ErrReadValue = errors.New("Error, unexpected read error")

//var ErrCollision = errors.New("Error, hash collision") // don't happen, on top layer of database
var counters sync.Map
var mutex = &sync.RWMutex{} //global mutex for counters and so on

// Store struct
// data sharded by chunks
type Store struct {
	chunks       [chunksCnt]chunk
	dir          string
	syncInterval time.Duration
}

type addrSize struct {
	addr uint32
	size byte
}

// chunk
type chunk struct {
	sync.RWMutex
	f  *os.File            // file storage
	m  map[uint32]addrSize // keys: hash / addr&len
	h  map[uint32]byte     // holes: addr / size
	iv interval.Interval
}

// Walk struct
type WalkFn func(key []byte, val []byte) bool

// Option is a function that takes a pointer to a Store and returns an error if it is invalid
type Option func(*Store) error

// SyncInterval - how often fsync do, default 0 = disabled
func SyncInterval(interv time.Duration) Option {
	return func(s *Store) error {
		s.syncInterval = interv
		return nil
	}
}

// Dir - directory for database, default "."
func Dir(dir string) Option {
	return func(s *Store) error {
		if dir == "" {
			dir = "."
		}
		var err error
		// create dirs
		_, err = os.Stat(dir)
		if err != nil {
			// file not exists - create dirs if any
			if os.IsNotExist(err) {
				if dir != "." {
					err = os.MkdirAll(dir, os.FileMode(dirMode))
					if err != nil {
						return err
					}
				}
			} else {
				return err
			}
		}
		s.dir = dir
		return nil
	}
}

func hash(b []byte) uint32 {
	return murmur3.Sum32WithSeed(b, 0)
	/*
		convert to 24 bit hash
		//MASK_24 := uint32((1 << 24) - 1)
		//ss := h.Sum32()
		//hash := (ss >> 24) ^ (ss & MASK_24)
	*/
}

func (c *chunk) init(name string, syncInterval time.Duration) (err error) {
	c.Lock()
	defer c.Unlock()

	f, err := os.OpenFile(name, os.O_CREATE|os.O_RDWR, os.FileMode(fileMode))
	if err != nil {
		return
	}
	err = f.Sync()
	if err != nil {
		return err
	}
	c.f = f
	c.m = make(map[uint32]addrSize)
	c.h = make(map[uint32]byte)

	if syncInterval > 0 {
		c.iv = interval.Set(func(t time.Time) {
			err = c.f.Sync()
			if err != nil {
				//not possible, if drive ok, may be panic here??
				fmt.Printf("Error fsync:%s\n", err)
			}
		}, syncInterval)
	}
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
				return ErrFormat
			}
			// skip val
			_, seekerr := c.f.Seek(int64(lenv), 1)
			if seekerr != nil {
				return ErrFormat
			}
			// read key
			key := make([]byte, lenk)
			n, errRead = c.f.Read(key)
			if errRead != nil || n != int(lenk) {
				return ErrFormat
			}
			shiftv := 1 << byte(b[0])                                               //2^pow
			ret, seekerr := c.f.Seek(int64(shiftv-int(lenk)-int(lenv)-sizeHead), 1) // skip val && key
			if seekerr != nil {
				return ErrFormat
			}
			// map store
			if b[1] != deleted {
				h := hash(key)
				c.m[h] = addrSizeMarshal(uint32(seek), b[0])
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
// It will create 256 shards
// Each shard store keys and val size and address in map[uint32]uint32
//
// options, see https://gist.github.com/travisjeffery/8265ca411735f638db80e2e34bdbd3ae#gistcomment-3171484
func Open(opts ...Option) (s *Store, err error) {
	s = &Store{}

	s.syncInterval = 0
	// call option functions on instance to set options on it
	for _, opt := range opts {
		err := opt(s)
		// if the option func returns an error, add it to the list of errors
		if err != nil {
			return nil, err
		}
	}

	// create chuncks
	for i := range s.chunks[:] {

		err = s.chunks[i].init(fmt.Sprintf("%s/%d", s.dir, i), s.syncInterval)
		if err != nil {
			return nil, err
		}
	}
	return
}

// pack addr & size to addrSize
func addrSizeMarshal(addr uint32, size byte) addrSize {
	return addrSize{addr, size}
}

// unpack addr & size
func addrSizeUnmarshal(as addrSize) (addr, size uint32) {
	return as.addr, 1 << as.size
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

// Set - store key and val in shard
// max packet size is 2^19, 512kb (524288)
// packet size = len(key) + len(val) + 8
func (s *Store) Set(k, v []byte) (err error) {
	h := hash(k)
	idx := idx(h)
	err = s.chunks[idx].set(k, v, h)
	if err == ErrCollision {
		for i := 0; i < chunkColCnt; i++ {
			err = s.chunks[i].set(k, v, h)
			if err == ErrCollision {
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
		addr, size := addrSizeUnmarshal(addrsize)
		packet := make([]byte, size)
		_, err = c.f.ReadAt(packet, int64(addr))
		if err != nil {
			return err
		}
		key, _, sizeold := packetUnmarshal(packet)
		if !bytes.Equal(key, k) {
			//println(string(key), string(k))
			return ErrCollision
		}

		if err == nil && sizeold == sizeb {
			//overwrite
			pos = int64(addr)
		} else {
			// mark old k/v as deleted
			delb := make([]byte, 1)
			delb[0] = deleted
			_, err = c.f.WriteAt(delb, int64(addr+1))
			if err != nil {
				return err
			}
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
	_, err = c.f.WriteAt(b, pos)
	if err != nil {
		return err
	}
	c.m[h] = addrSizeMarshal(uint32(pos), sizeb)
	return
}

// Get - return val by key
func (s *Store) Get(k []byte) (v []byte, err error) {
	h := hash(k)
	idx := idx(h)
	v, err = s.chunks[idx].get(k, h)
	if err == ErrCollision {
		for i := 0; i < chunkColCnt; i++ {
			v, err = s.chunks[i].get(k, h)
			if err == ErrCollision || err == ErrNotFound {
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
		addr, size := addrSizeUnmarshal(addrsize)
		packet := make([]byte, size)
		_, err = c.f.ReadAt(packet, int64(addr))
		if err != nil {
			return
		}
		key, val, _ := packetUnmarshal(packet)
		if !bytes.Equal(key, k) {
			return nil, ErrCollision
		}
		v = val
	} else {
		return nil, ErrNotFound
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

// Walk sequential return key/values on the fly to external walk app function
func (s *Store) Walk(fn WalkFn) (err error) {
	for idx := range s.chunks[:] {
		_, err = s.chunks[idx].walk(fn)
		if err != nil {
			return
		}
	}
	return
}

// Walk parallel return key/values on the fly to external walk app function
func (s *Store) WalkPll(fn WalkFn, threads int) (err error) {

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	qwait := make(chan bool)

	qwg, ctx := waitgroup.NewErrorGroup(ctx, threads)

Main:

	for idx := range s.chunks[:] {

		select {
		case <-ctx.Done():
			err = ctx.Err()
			break Main
		default:
		}

		qwg.Add(func() (err error) {

			qidx := idx

			qwait <- true

			_, err = s.chunks[qidx].walk(fn)
			if err != nil {
				cancel()
				return
			}

			return

		})

		<-qwait

	}

	err = qwg.Wait()
	if err != nil {
		return
	}

	return

}

// Internal sequential walk over all chunks
func (c *chunk) walk(fn WalkFn) (bool, error) {
	c.RLock()
	defer c.RUnlock()
	for h, _ := range c.m {
		if addrsize, ok := c.m[h]; ok {
			addr, size := addrSizeUnmarshal(addrsize)
			packet := make([]byte, size)
			_, err := c.f.ReadAt(packet, int64(addr))
			if err != nil {
				return true, ErrReadValue
			}
			key, val, _ := packetUnmarshal(packet)
			fn(key, val)
		} else {
			return true, ErrAddrSize
		}
	}
	return false, nil
}

// Close - close related chunks
func (s *Store) Close() (err error) {
	errStr := ""
	for i := range s.chunks[:] {
		err = s.chunks[i].close()
		if err != nil {
			errStr += err.Error() + "\r\n"
			return
		}
	}
	if errStr != "" {
		return errors.New(errStr)
	}
	return
}

// close file
func (c *chunk) close() (err error) {
	c.Lock()
	defer c.Unlock()
	//c.iv.Clear()

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
func (s *Store) Delete(k []byte) (isDeleted bool, err error) {
	h := hash(k)
	idx := idx(h)
	isDeleted, err = s.chunks[idx].delete(k, h)
	if err == ErrCollision {
		for i := 0; i < chunkColCnt; i++ {
			isDeleted, err = s.chunks[i].delete(k, h)
			if err == ErrCollision || err == ErrNotFound {
				continue
			}
			break
		}
	}
	return
}

// delete mark item as deleted at specified position
func (c *chunk) delete(k []byte, h uint32) (isDeleted bool, err error) {
	c.Lock()
	defer c.Unlock()
	if addrsize, ok := c.m[h]; ok {
		addr, size := addrSizeUnmarshal(addrsize)
		packet := make([]byte, size)
		_, err = c.f.ReadAt(packet, int64(addr))
		if err != nil {
			return
		}
		key, _, sizeb := packetUnmarshal(packet)
		if !bytes.Equal(key, k) {
			return false, ErrCollision
		}

		delb := make([]byte, 1)
		delb[0] = deleted
		_, err = c.f.WriteAt(delb, int64(addr+1))
		if err != nil {
			return
		}
		delete(c.m, h)
		c.h[addr] = sizeb
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

// TODO - optimize
func (c *chunk) incrdecr(k []byte, h uint32, v uint64, isIncr bool) (counter uint64, err error) {
	mutex.Lock()
	defer mutex.Unlock()
	old, err := c.get(k, h)
	if err == ErrNotFound {
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

// Backup is very stupid now. It remove files with same name with bak extension
// and create new backup files
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
