package sniper

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
)

// chunk - local shard
type chunk struct {
	sync.RWMutex
	f         *os.File            // file storage
	m         map[uint32]addrSize // keys: hash / addr&len
	h         map[uint32]byte     // holes: addr / size
	needFsync bool
}

type addrSize struct {
	addr uint32
	size byte
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

func (c *chunk) init(name string) (err error) {
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
				return fmt.Errorf("len(key) == 0: %w", ErrFormat)
			}
			// skip val
			_, seekerr := c.f.Seek(int64(lenv), 1)
			if seekerr != nil {
				return fmt.Errorf("%s: %w", seekerr.Error(), ErrFormat)
			}
			// read key
			key := make([]byte, lenk)
			n, errRead = c.f.Read(key)
			if errRead != nil || n != int(lenk) {
				if errRead != nil {
					return fmt.Errorf("%s: %w", errRead.Error(), ErrFormat)
				}
				return fmt.Errorf("n != int(lenk): %w", ErrFormat)
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

//fsync commits the current contents of the file to stable storage
func (c *chunk) fsync() error {
	if c.needFsync {
		c.Lock()
		defer c.Unlock()
		c.needFsync = false
		return c.f.Sync()
	}
	return nil
}

// set - write data to file & in map
func (c *chunk) set(k, v []byte, h uint32) (err error) {
	c.Lock()
	defer c.Unlock()
	c.needFsync = true
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

// return map length
func (c *chunk) count() int {
	c.RLock()
	defer c.RUnlock()
	return len(c.m)
}

// close file
func (c *chunk) close() (err error) {
	c.Lock()
	defer c.Unlock()

	return c.f.Close()
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
	err = dest.Sync()
	if err != nil {
		return
	}
	return dest.Close()
}
