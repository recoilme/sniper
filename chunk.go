package sniper

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"
)

const (
	currentChunkVersion = 1
	versionMarker       = 255
	deleted             = 42 // flag for removed, tribute 2 dbf
)

var (
	sizeHeaders = map[int]uint32{0: 8, 1: 12}
	sizeHead    = sizeHeaders[currentChunkVersion]
)

// chunk - local shard
type chunk struct {
	sync.RWMutex
	f         *os.File          // file storage
	m         map[uint32]uint64 // keys: hash / key meta info
	h         map[uint32]byte   // holes: addr / size
	needFsync bool
}

type Header struct {
	sizeb  uint8
	status uint8
	keylen uint16
	vallen uint32
	expire uint32
}

func encodeKeyMeta(addr uint32, size byte, expire int64) uint64 {
	return uint64(addr)<<32 | uint64(size)<<24 | uint64(expire)>>9 + 1
}

func decodeKeyMeta(info uint64) (addr uint32, size byte, expire int64) {
	addr = uint32(info >> 32)
	size = byte(info >> 24 & 0xff)
	expire = int64(info&0xffffff) << 9
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
	if v == 0 {
		return 0, 0
	}
	for power = 0; power < 32; power++ {
		val = 1 << power
		if val >= v {
			break
		}
	}
	if power == 32 {
		//overflow
		val = 4294967295
	}
	return
}

func detectChunkVersion(file *os.File) (version int, err error) {
	b := make([]byte, 2)
	n, errRead := file.Read(b)
	if errRead != nil {
		return -1, errRead
	}
	if n != 2 {
		return -1, errors.New("File too short")
	}

	// 255 version marker
	if b[0] == versionMarker {
		if b[1] == 0 || b[1] == deleted {
			// first version
			return 0, nil
		}
		return int(b[1]), nil
	}
	if b[1] == 0 || b[1] == deleted {
		// first version
		return 0, nil
	}
	return -1, nil
}

func makeHeader(k, v []byte, expire uint32) (header *Header) {
	header = &Header{}
	header.status = 0
	header.keylen = uint16(len(k))
	header.vallen = uint32(len(v))
	header.expire = expire
	sizeb, _ := NextPowerOf2(uint32(header.keylen) + header.vallen + sizeHead)
	header.sizeb = sizeb
	return
}

func parseHeaderV0(b []byte) (header *Header) {
	header = &Header{}
	header.sizeb = b[0]
	header.status = b[1]
	header.keylen = binary.BigEndian.Uint16(b[2:4])
	header.vallen = binary.BigEndian.Uint32(b[4:8])
	return
}

func parseHeader(b []byte) (header *Header) {
	header = &Header{}
	header.sizeb = b[0]
	header.status = b[1]
	header.keylen = binary.BigEndian.Uint16(b[2:4])
	header.vallen = binary.BigEndian.Uint32(b[4:8])
	header.expire = binary.BigEndian.Uint32(b[8:12])
	return
}

func readHeader(r io.Reader, version int) (header *Header, err error) {
	b := make([]byte, sizeHeaders[version])
	n, err := io.ReadFull(r, b)
	if n != int(sizeHeaders[version]) {
		if err == io.EOF {
			err = nil
		}
		return
	}
	switch version {
	case 0:
		header = parseHeaderV0(b)
	case currentChunkVersion:
		header = parseHeader(b)
	default:
		err = fmt.Errorf("Unknov header version %d", version)
	}
	return
}

func writeHeader(b []byte, header *Header) {
	b[0] = header.sizeb
	b[1] = header.status
	binary.BigEndian.PutUint16(b[2:4], header.keylen)
	binary.BigEndian.PutUint32(b[4:8], header.vallen)
	binary.BigEndian.PutUint32(b[8:12], header.expire)
	return
}

func packetMarshal(k, v []byte, expire uint32) (header *Header, b []byte) {
	// write head
	header = makeHeader(k, v, expire)
	size := 1 << header.sizeb
	b = make([]byte, size)
	writeHeader(b, header)
	// write body: val and key
	copy(b[sizeHead:], v)
	copy(b[sizeHead+header.vallen:], k)
	return
}

func packetUnmarshal(packet []byte) (header *Header, k, v []byte) {
	header = parseHeader(packet)
	k = packet[sizeHead+header.vallen : sizeHead+header.vallen+uint32(header.keylen)]
	v = packet[sizeHead : sizeHead+header.vallen]
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
	c.m = make(map[uint32]uint64)
	c.h = make(map[uint32]byte)
	//read if f not empty
	if fi, e := c.f.Stat(); e == nil {
		// new file
		if fi.Size() == 0 {
			// write chunk version info
			c.f.Write([]byte{versionMarker, currentChunkVersion})
			return
		}

		//read file
		var seek int
		// detect chunk version
		version, errDetect := detectChunkVersion(c.f)
		if errDetect != nil {
			err = errDetect
			return
		}

		if version < 0 || version > currentChunkVersion {
			err = errors.New("Unknown chunk version in file " + name)
			return
		}

		if version == 0 {
			// rewind to begin
			c.f.Seek(0, 0)
		} else {
			// real chunk begin
			seek = 2
		}

		if version > currentChunkVersion {
			err = fmt.Errorf("chunk %s unsupported version %d", name, version)
		}

		// if load chunk with old version create file in new format
		if version < currentChunkVersion {
			var newfile *os.File
			fmt.Printf("Load from old version chunk %s, do inplace upgrade v%d -> v%d\n", name, version, currentChunkVersion)
			newname := name + ".new"
			newfile, err = os.OpenFile(newname, os.O_CREATE|os.O_RDWR|os.O_TRUNC, os.FileMode(fileMode))
			if err != nil {
				return
			}
			// write chunk version info
			newfile.Write([]byte{versionMarker, currentChunkVersion})
			seek := 2
			oldsizehead := sizeHeaders[version]
			sizediff := sizeHead - oldsizehead
			for {
				var header *Header
				var errRead error
				header, errRead = readHeader(c.f, version)
				if errRead != nil {
					newfile.Close()
					return errRead
				}
				if header == nil {
					break
				}
				oldsizedata := (1 << header.sizeb) - oldsizehead
				sizeb, size := NextPowerOf2(uint32(sizeHead) + uint32(header.keylen) + header.vallen)
				header.sizeb = sizeb
				b := make([]byte, size+sizediff)
				writeHeader(b, header)
				n, errRead := c.f.Read(b[sizeHead : sizeHead+oldsizedata])
				if errRead != nil {
					return fmt.Errorf("%s: %w", errRead.Error(), ErrFormat)
				}
				if n != int(oldsizedata) {
					return fmt.Errorf("n != record length: %w", ErrFormat)
				}

				// skip deleted or expired entry
				if header.status == deleted || (header.expire != 0 && int64(header.expire) < time.Now().Unix()) {
					continue
				}
				keyidx := int(sizeHead) + int(header.vallen)
				h := hash(b[keyidx : keyidx+int(header.keylen)])
				c.m[h] = encodeKeyMeta(uint32(seek), header.sizeb, int64(header.expire))
				n, errRead = newfile.Write(b[0:size])
				if errRead != nil {
					return fmt.Errorf("%s: %w", errRead.Error(), ErrFormat)
				}
				seek += n
			}
			// close old chunk file
			errRead := c.f.Close()
			if errRead != nil {
				return fmt.Errorf("%s: %w", errRead.Error(), ErrFormat)
			}
			// set new file for chunk
			c.f = newfile
			// remove old chunk file from disk
			errRead = os.Remove(name)
			if errRead != nil {
				return fmt.Errorf("%s: %w", errRead.Error(), ErrFormat)
			}
			// rename new file to old file
			errRead = os.Rename(newname, name)
			if errRead != nil {
				return fmt.Errorf("%s: %w", errRead.Error(), ErrFormat)
			}
			return
		}

		var n int
		for {
			header, errRead := readHeader(c.f, version)
			if errRead != nil {
				return fmt.Errorf("%s: %w", errRead.Error(), ErrFormat)
			}
			if header == nil {
				break
			}
			// skip val
			_, seekerr := c.f.Seek(int64(header.vallen), 1)
			if seekerr != nil {
				return fmt.Errorf("%s: %w", seekerr.Error(), ErrFormat)
			}
			// read key
			key := make([]byte, header.keylen)
			n, errRead = c.f.Read(key)
			if errRead != nil {
				return fmt.Errorf("%s: %w", errRead.Error(), ErrFormat)
			}
			if n != int(header.keylen) {
				return fmt.Errorf("n != key length: %w", ErrFormat)
			}
			shiftv := 1 << header.sizeb                                                                    //2^pow
			ret, seekerr := c.f.Seek(int64(shiftv-int(header.keylen)-int(header.vallen)-int(sizeHead)), 1) // skip empty tail
			if seekerr != nil {
				return ErrFormat
			}
			// map store
			if header.status != deleted && (header.expire == 0 || int64(header.expire) >= time.Now().Unix()) {
				h := hash(key)
				c.m[h] = encodeKeyMeta(uint32(seek), header.sizeb, int64(header.expire))
			} else {
				//deleted blocks store
				c.h[uint32(seek)] = header.sizeb // seek / size
			}
			seek = int(ret)
		}
	}

	return
}

// fsync commits the current contents of the file to stable storage
func (c *chunk) fsync() error {
	if c.needFsync {
		c.Lock()
		defer c.Unlock()
		c.needFsync = false
		return c.f.Sync()
	}
	return nil
}

// expirekeys walk all keys and delete expired
// maxruntime - maximum run time
func (c *chunk) expirekeys(maxruntime time.Duration) error {
	starttime := time.Now().UnixMilli()
	curtime := starttime / 1000
	expiredlist := make([]uint32, 1024)
	if maxruntime.Seconds() > 1000 {
		maxruntime = time.Duration(1000) * time.Second
	}
	stoptime := starttime + maxruntime.Milliseconds()

	c.RLock()
	for h, meta := range c.m {
		_, _, expire := decodeKeyMeta(meta)
		if expire != 0 && curtime > expire {
			expiredlist = append(expiredlist, h)
		}
	}
	c.RUnlock()
	keycount := len(expiredlist)
	if keycount == 0 {
		return nil
	}

	sleeptime := maxruntime.Milliseconds() / int64(keycount)
	if sleeptime < 1 {
		sleeptime = 1
	} else if sleeptime > 10 {
		sleeptime = 10
	}

	// special case, expire all keys at maximum speed
	// maximum run time 300s
	if maxruntime == time.Duration(0) {
		sleeptime = 0
		stoptime = starttime + 300000
	}

	for _, h := range expiredlist {
		if time.Now().UnixMilli() >= stoptime {
			break
		}
		if meta, ok := c.m[h]; ok {
			c.Lock()
			addr, _, _ := decodeKeyMeta(meta)
			headerbuf := make([]byte, sizeHead)
			_, err := c.f.ReadAt(headerbuf, int64(addr))
			if err != nil {
				c.Unlock()
				return err
			}
			header := parseHeader(headerbuf)
			if header.expire != 0 && curtime > int64(header.expire) {
				delb := []byte{deleted}
				_, err = c.f.WriteAt(delb, int64(addr+1))
				if err != nil {
					c.Unlock()
					return err
				}
				delete(c.m, h)
				c.h[addr] = header.sizeb
			}
			c.Unlock()
			time.Sleep(time.Duration(sleeptime) * time.Millisecond)
		}
	}
	return nil
}

// set - write data to file & in map
func (c *chunk) set(k, v []byte, h uint32, expire uint32) (err error) {
	c.Lock()
	defer c.Unlock()
	err = c.write_key(k, v, h, expire)
	return
}

func (c *chunk) write_key(k, v []byte, h uint32, expire uint32) (err error) {
	c.needFsync = true
	header, b := packetMarshal(k, v, expire)
	// write at file
	pos := int64(-1)

	if meta, ok := c.m[h]; ok {
		addr, size, _ := decodeKeyMeta(meta)
		packet := make([]byte, 1<<size)
		_, err = c.f.ReadAt(packet, int64(addr))
		if err != nil {
			return err
		}
		headerold, key, _ := packetUnmarshal(packet)
		if !bytes.Equal(key, k) {
			//println(string(key), string(k))
			return ErrCollision
		}

		if headerold.sizeb == header.sizeb {
			//overwrite
			pos = int64(addr)
		} else {
			// mark old k/v as deleted
			delb := []byte{deleted}
			_, err = c.f.WriteAt(delb, int64(addr+1))
			if err != nil {
				return err
			}
			c.h[addr] = headerold.sizeb

			// try to find optimal empty hole
			for addrh, sizeh := range c.h {
				if sizeh == header.sizeb {
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
	c.m[h] = encodeKeyMeta(uint32(pos), header.sizeb, int64(header.expire))
	return
}

// touch - write data to file & in map
func (c *chunk) touch(k []byte, h uint32, expire uint32) (err error) {
	c.Lock()
	defer c.Unlock()

	if meta, ok := c.m[h]; ok {
		addr, size, _ := decodeKeyMeta(meta)
		packet := make([]byte, 1<<size)
		_, err = c.f.ReadAt(packet, int64(addr))
		if err != nil {
			return err
		}
		header, key, _ := packetUnmarshal(packet)
		if !bytes.Equal(key, k) {
			return ErrCollision
		}
		if header.expire != 0 && int64(header.expire) < time.Now().Unix() {
			return ErrNotFound
		}

		header.expire = expire
		b := make([]byte, sizeHead)
		writeHeader(b, header)
		_, err = c.f.WriteAt(b, int64(addr))
		if err != nil {
			return err
		}
		c.needFsync = true

	} else {
		return ErrNotFound
	}
	return
}

// get return val by key
func (c *chunk) get(k []byte, h uint32) (v []byte, header *Header, err error) {
	c.RLock()
	defer c.RUnlock()
	v, header, err = c.load_key(k, h)
	return
}

func (c *chunk) load_key(k []byte, h uint32) (v []byte, header *Header, err error) {
	if meta, ok := c.m[h]; ok {
		addr, size, _ := decodeKeyMeta(meta)
		packet := make([]byte, 1<<size)
		_, err = c.f.ReadAt(packet, int64(addr))
		if err != nil {
			return
		}
		header, key, val := packetUnmarshal(packet)
		if !bytes.Equal(key, k) {
			return nil, nil, ErrCollision
		}
		if header.expire != 0 && int64(header.expire) < time.Now().Unix() {
			c.RUnlock()
			_, err := c.delete(k, h)
			c.RLock()
			if err != nil {
				return nil, nil, err
			}
			return nil, nil, ErrNotFound
		}
		v = val
	} else {
		return nil, nil, ErrNotFound
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
	if meta, ok := c.m[h]; ok {
		addr, size, _ := decodeKeyMeta(meta)
		packet := make([]byte, 1<<size)
		_, err = c.f.ReadAt(packet, int64(addr))
		if err != nil {
			return
		}
		header, key, _ := packetUnmarshal(packet)
		if !bytes.Equal(key, k) {
			return false, ErrCollision
		}

		delb := []byte{deleted}
		_, err = c.f.WriteAt(delb, int64(addr+1))
		if err != nil {
			return
		}
		delete(c.m, h)
		c.h[addr] = header.sizeb
		isDeleted = true
	}
	return
}

// TODO - optimize
func (c *chunk) incrdecr(k []byte, h uint32, v uint64, isIncr bool) (counter uint64, err error) {
	c.Lock()
	defer c.Unlock()
	old, header, err := c.load_key(k, h)
	expire := uint32(0)
	if header != nil {
		expire = header.expire
	}

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
	err = c.write_key(k, new, h, expire)

	return
}

func (c *chunk) backup(w io.Writer) (err error) {
	c.Lock()
	defer c.Unlock()
	_, seekerr := c.f.Seek(2, 0)
	if seekerr != nil {
		return fmt.Errorf("%s: %w", seekerr.Error(), ErrFormat)
	}

	for {
		var header *Header
		var errRead error
		header, errRead = readHeader(c.f, currentChunkVersion)
		if errRead != nil {
			return errRead
		}
		if header == nil {
			break
		}
		size := int(sizeHead) + int(header.vallen) + int(header.keylen) // record size
		b := make([]byte, size)
		writeHeader(b, header)
		n, errRead := c.f.Read(b[sizeHead:])
		if errRead != nil {
			return fmt.Errorf("%s: %w", errRead.Error(), ErrFormat)
		}
		if n != size-int(sizeHead) {
			return fmt.Errorf("n != record length: %d != %d %w", n, size-int(sizeHead), ErrFormat)
		}

		shiftv := 1 << header.sizeb                                                                  //2^pow
		_, seekerr := c.f.Seek(int64(shiftv-int(header.keylen)-int(header.vallen)-int(sizeHead)), 1) // skip empty tail
		if seekerr != nil {
			return ErrFormat
		}

		// skip deleted or expired entry
		if header.status == deleted || (header.expire != 0 && int64(header.expire) < time.Now().Unix()) {
			continue
		}
		n, errRead = w.Write(b)
		if errRead != nil {
			return fmt.Errorf("%s: %w", errRead.Error(), ErrFormat)
		}
	}
	return nil
}
