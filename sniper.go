package sniper

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

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

// ErrFormat unexpected file format
var ErrFormat = errors.New("Error, unexpected file format")

// ErrNotFound key not found error
var ErrNotFound = errors.New("Error, key not found")

var counters sync.Map
var mutex = &sync.RWMutex{} //global mutex for counters and so on

// Store struct
// data sharded by chunks
type Store struct {
	chunks       [chunksCnt]chunk
	dir          string
	syncInterval time.Duration
	iv           interval.Interval
}

type addrSize struct {
	addr uint32
	size byte
}

// OptStore is a store options
type OptStore func(*Store) error

// Dir - directory for database, default "."
func Dir(dir string) OptStore {
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

// SyncInterval - how often fsync do, default 0 - OS will do it
func SyncInterval(interv time.Duration) OptStore {
	return func(s *Store) error {
		s.syncInterval = interv
		if interv > 0 {
			s.iv = interval.Set(func(t time.Time) {
				for i := range s.chunks[:] {
					err := s.chunks[i].fsync()
					if err != nil {
						fmt.Printf("Error fsync:%s\n", err)
						//critical error, ssd/hdd drive is broken
						panic(err)
					}
				}
			}, interv)
		}
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

// Open return new store
// It will create 256 shards
// Each shard store keys and val size and address in map[uint32]uint32
//
// options, see https://gist.github.com/travisjeffery/8265ca411735f638db80e2e34bdbd3ae#gistcomment-3171484
// usage - Open(Dir("1"), SyncInterval(1*time.Second))
func Open(opts ...OptStore) (s *Store, err error) {
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

		err = s.chunks[i].init(fmt.Sprintf("%s/%d", s.dir, i))
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

// Count return count keys
func (s *Store) Count() (cnt int) {
	for i := range s.chunks[:] {
		cnt += s.chunks[i].count()
	}
	return
}

// Close - close related chunks
func (s *Store) Close() (err error) {
	errStr := ""
	if s.syncInterval > 0 {
		s.iv.Clear()
	}
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
