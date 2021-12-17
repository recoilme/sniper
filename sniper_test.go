package sniper

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/tidwall/lotsa"
)

func TestPack(t *testing.T) {
	addr := 1<<26 - 5
	size := byte(5)
	some32 := addrSizeMarshal(uint32(addr), size)
	s, l := addrSizeUnmarshal(some32)
	if s != uint32(addr) || l != 32 {
		t.Errorf("get addr = %d, size=%d", s, l)
	}
	addr = 1<<28 - 1
	size = byte(19)
	maxAddrSize := addrSizeMarshal(uint32(addr), size)
	s, l = addrSizeUnmarshal(maxAddrSize)
	if s != uint32(addr) || l != 524288 {
		t.Errorf("get addr = %d, size=%d ", s, l)
	}
}

func TestHashCol(t *testing.T) {
	//println(1 << 32)
	k2 := make([]byte, 8)
	binary.BigEndian.PutUint64(k2, uint64(16_123_243))
	k3 := make([]byte, 8)
	binary.BigEndian.PutUint64(k3, uint64(106_987_520))
	println(hash(k2), hash(k3))
	//mgdbywinfo uzmqkfjche 720448991
	println("str", hash([]byte("mgdbywinfo")), hash([]byte("uzmqkfjche")))
	//		 4_294_967_296
	sizet := 100_000_000
	m := make(map[uint32]int, sizet)
	for i := 0; i < sizet; i++ {
		k1 := make([]byte, 8)
		binary.BigEndian.PutUint64(k1, uint64(i))
		h := hash(k1)
		if _, ok := m[h]; ok {
			println("collision", h, i, m[h])
			break
		}
		m[h] = i
	}

}
func TestPower(t *testing.T) {

	p, v := NextPowerOf2(256)
	if p != 8 || v != 256 {
		t.Errorf("get p = %d,v=%d want 8,256", p, v)
	}

	p, v = NextPowerOf2(1023)
	if p != 10 || v != 1024 {
		t.Errorf("get p = %d,v=%d want 10,1024", p, v)
	}

	p, v = NextPowerOf2(4294967294) //2^32-1-1
	if p != 32 || v != 4294967295 {
		t.Errorf("get p = %d,v=%d want 33,4294967295", p, v)
	}

	p, v = NextPowerOf2(3)
	if p != 2 || v != 4 {
		t.Errorf("get p = %d,v=%d want 2,4", p, v)
	}
	p, v = NextPowerOf2(0)
	if p != 0 || v != 0 {
		t.Errorf("get p = %d,v=%d want 0,0", p, v)
	}
}

func TestCmd(t *testing.T) {
	err := DeleteStore("1")
	assert.NoError(t, err)

	s, err := Open(Dir("1"))
	assert.NoError(t, err)

	err = s.Set([]byte("hello"), []byte("go"), 0)
	assert.NoError(t, err)

	err = s.Set([]byte("hello"), []byte("world"), 0)
	assert.NoError(t, err)

	res, err := s.Get([]byte("hello"))
	assert.NoError(t, err)

	assert.Equal(t, true, bytes.Equal(res, []byte("world")))

	assert.Equal(t, 1, s.Count())

	err = s.Close()
	assert.NoError(t, err)

	s, err = Open(Dir("1"))
	assert.NoError(t, err)

	res, err = s.Get([]byte("hello"))
	assert.NoError(t, err)

	assert.Equal(t, true, bytes.Equal(res, []byte("world")))
	assert.Equal(t, 1, s.Count())

	deleted, err := s.Delete([]byte("hello"))
	assert.NoError(t, err)
	assert.True(t, deleted)
	assert.Equal(t, 0, s.Count())

	counter := []byte("counter")

	cnt, err := s.Incr(counter, uint64(1))
	assert.NoError(t, err)
	assert.Equal(t, 1, int(cnt))
	cnt, err = s.Incr(counter, uint64(42))
	assert.NoError(t, err)
	assert.Equal(t, 43, int(cnt))

	cnt, err = s.Decr(counter, uint64(2))
	assert.NoError(t, err)
	assert.Equal(t, 41, int(cnt))

	//overflow
	cnt, err = s.Decr(counter, uint64(42))
	assert.NoError(t, err)
	assert.Equal(t, uint64(18446744073709551615), uint64(cnt))

	err = s.Close()
	assert.NoError(t, err)

	err = DeleteStore("1")
	assert.NoError(t, err)

	sniperBench(seed(100_000))
}

func randKey(rnd *rand.Rand, n int) []byte {
	s := make([]byte, n)
	rnd.Read(s)
	for i := 0; i < n; i++ {
		s[i] = 'a' + (s[i] % 26)
	}
	return s
}

func seed(N int) ([][]byte, int) {
	seed := int64(1570109110136449000) //time.Now().UnixNano() //1570108152262917000
	// println(seed)
	rng := rand.New(rand.NewSource(seed))

	K := 10

	fmt.Printf("\n")
	fmt.Printf("go version %s %s/%s\n", runtime.Version(), runtime.GOOS, runtime.GOARCH)
	fmt.Printf("\n")
	fmt.Printf("     number of cpus: %d\n", runtime.NumCPU())
	fmt.Printf("     number of keys: %d\n", N)
	fmt.Printf("            keysize: %d\n", K)
	fmt.Printf("        random seed: %d\n", seed)

	fmt.Printf("\n")

	keysm := make(map[string]bool, N)
	for len(keysm) < N {
		keysm[string(randKey(rng, K))] = true
	}
	keys := make([][]byte, 0, N)
	for key := range keysm {
		keys = append(keys, []byte(key))
	}
	return keys, N
}

func sniperBench(keys [][]byte, N int) {
	lotsa.Output = os.Stdout
	lotsa.MemUsage = true

	fmt.Println("-- sniper --")
	DeleteStore("1")
	s, err := Open(Dir("1")) //, SyncInterval(1*time.Second))
	if err != nil {
		panic(err)
	}
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)

	fmt.Printf("Alloc = %v MiB Total = %v MiB\n", (ms.Alloc / 1024 / 1024), (ms.TotalAlloc / 1024 / 1024))

	fmt.Print("set: ")
	coll := 0
	lotsa.Ops(N, runtime.NumCPU(), func(i, _ int) {
		b := make([]byte, 8)
		binary.BigEndian.PutUint64(b, uint64(i))
		//println("set", i, keys[i], b)
		err := s.Set(keys[i], b, 0)
		if err == ErrCollision {
			coll++
			err = nil
		}
		if err != nil {
			panic(err)
		}
	})
	runtime.ReadMemStats(&ms)

	fmt.Printf("Alloc = %v MiB Total = %v MiB Coll=%d\n", (ms.Alloc / 1024 / 1024), (ms.TotalAlloc / 1024 / 1024), coll)
	coll = 0
	fmt.Print("get: ")
	lotsa.Ops(N, runtime.NumCPU(), func(i, _ int) {
		b, err := s.Get(keys[i])
		if err != nil {
			println("errget", string(keys[i]))
			panic(err)
		}
		v := binary.BigEndian.Uint64(b)

		if uint64(i) != v {
			println("get error:", string(keys[i]), i, v)
			panic("bad news")
		}
	})

	runtime.ReadMemStats(&ms)

	fmt.Printf("Alloc = %v MiB Total = %v MiB\n", (ms.Alloc / 1024 / 1024), (ms.TotalAlloc / 1024 / 1024))

	fmt.Print("del: ")
	lotsa.Ops(N, runtime.NumCPU(), func(i, _ int) {
		s.Delete(keys[i])
	})
	err = DeleteStore("1")
	if err != nil {
		panic("bad news")
	}
}

func TestSync(t *testing.T) {
	sniperBench(seed(100_000))
}

func TestSingleFile(t *testing.T) {
	DeleteStore("2")
	s, err := Open(Dir("2"), ChunksCollision(0), ChunksTotal(1))
	assert.NoError(t, err)
	err = s.Set([]byte("mgdbywinfo"), []byte("1"), 0)
	assert.NoError(t, err)
	err = s.Set([]byte("uzmqkfjche"), []byte("2"), 0)
	assert.NoError(t, err)

	v, err := s.Get([]byte("uzmqkfjche"))
	assert.NoError(t, err)
	assert.EqualValues(t, []byte("2"), v)
	v, err = s.Get([]byte("mgdbywinfo"))
	assert.NoError(t, err)
	assert.EqualValues(t, []byte("1"), v)

	err = s.Close()
	assert.NoError(t, err)
	DeleteStore("2")
}

func TestBucket(t *testing.T) {
	DeleteStore("2")
	s, err := Open(Dir("2"), ChunksCollision(0), ChunksTotal(1))
	assert.NoError(t, err)
	users, err := s.Bucket("users")
	assert.NoError(t, err)

	err = s.Put(users, []byte("01"), []byte("rob"))
	assert.NoError(t, err)

	err = s.Put(users, []byte("02"), []byte("bob"))
	assert.NoError(t, err)

	v, err := s.Get([]byte("users01"))
	assert.NoError(t, err)
	assert.Equal(t, []byte("rob"), v)

	v, err = s.Get([]byte("users02"))
	assert.NoError(t, err)
	assert.Equal(t, []byte("bob"), v)

	assert.Equal(t, []string{"02", "01"}, users.Keys(0, 0))

	err = s.Close()
	assert.NoError(t, err)
	DeleteStore("2")
}

func TestEmptyKey(t *testing.T) {
	err := DeleteStore("1")
	assert.NoError(t, err)

	s, err := Open(Dir("1"))
	assert.NoError(t, err)

	err = s.Set([]byte(""), []byte("go"), 0)
	assert.NoError(t, err)

	err = s.Set([]byte(""), []byte("world"), 0)
	assert.NoError(t, err)

	res, err := s.Get([]byte(""))
	assert.NoError(t, err)

	assert.Equal(t, true, bytes.Equal(res, []byte("world")))

	assert.Equal(t, 1, s.Count())

	err = s.Close()
	assert.NoError(t, err)

	err = DeleteStore("1")
	assert.NoError(t, err)
}

func TestExpireKey(t *testing.T) {
	err := DeleteStore("1")
	assert.NoError(t, err)

	s, err := Open(Dir("1"))
	assert.NoError(t, err)

	unixtime := uint32(time.Now().Unix())

	// set key with expire 1 sec
	err = s.Set([]byte("key1"), []byte("go"), unixtime+1)
	assert.NoError(t, err)

	// set key with expire 3 sec
	err = s.Set([]byte("key2"), []byte("world"), unixtime+3)
	assert.NoError(t, err)

	// try get key1
	res, err := s.Get([]byte("key1"))
	assert.NoError(t, err)

	assert.Equal(t, true, bytes.Equal(res, []byte("go")))

	assert.Equal(t, 2, s.Count())

	// sleep 2 sec, key1 should expired
	time.Sleep(time.Second * 2)

	res, err = s.Get([]byte("key1"))
	assert.Equal(t, ErrNotFound, err)

	assert.Equal(t, 1, s.Count())

	// key2 must exist
	res, err = s.Get([]byte("key2"))
	assert.NoError(t, err)

	assert.Equal(t, true, bytes.Equal(res, []byte("world")))

	// sleep 2 sec, key1 should expired
	time.Sleep(time.Second * 2)

	res, err = s.Get([]byte("key2"))
	assert.Equal(t, ErrNotFound, err)

	// all keys expired
	assert.Equal(t, 0, s.Count())

	/* test Expire method */
	unixtime = uint32(time.Now().Unix())

	err = s.Set([]byte("key1"), []byte("go"), unixtime+1)
	assert.NoError(t, err)

	// sleep 2 sec, key1 should expired
	time.Sleep(time.Second * 2)
	err = s.Expire()
	assert.NoError(t, err)

	// all keys expired
	assert.Equal(t, 0, s.Count())

	/* test touch */
	unixtime = uint32(time.Now().Unix())

	err = s.Set([]byte("key"), []byte("go"), unixtime+4)
	assert.NoError(t, err)

	// sleep 2 sec, key1 should stay
	time.Sleep(time.Second * 2)
	res, err = s.Get([]byte("key"))
	assert.NoError(t, err)

	unixtime = uint32(time.Now().Unix())
	err = s.Touch([]byte("key"), unixtime+3)
	assert.NoError(t, err)

	// sleep 3 sec, key1 should stay
	time.Sleep(time.Second * 3)
	res, err = s.Get([]byte("key"))
	assert.NoError(t, err)

	// sleep 3 sec, key should expired
	time.Sleep(time.Second * 3)
	err = s.Expire()
	assert.NoError(t, err)

	// all keys expired
	assert.Equal(t, 0, s.Count())

	err = s.Close()
	assert.NoError(t, err)

	err = DeleteStore("1")
	assert.NoError(t, err)
}

func getRandKey(rnd *rand.Rand, n int) []byte {
	s := make([]byte, n)
	rnd.Read(s)
	for i := 0; i < n; i++ {
		s[i] = 'a' + (s[i] % 26)
	}
	return s
}

func TestBackup(t *testing.T) {
	var backup = "data1.backup.gz"

	f, _ := os.Create(backup)
	defer f.Close()

	err := DeleteStore("1")
	assert.NoError(t, err)

	s, err := Open(Dir("1"))
	assert.NoError(t, err)

	seed := time.Now().UnixNano()
	rng := rand.New(rand.NewSource(seed))
	coll := 0

	for i := 0; i < 1000000; i++ {
		b := make([]byte, 8)
		binary.BigEndian.PutUint64(b, uint64(i))
		err := s.Set(getRandKey(rng, 10), b, 0)
		if err == ErrCollision {
			coll++
			err = nil
		}
		if err != nil {
			panic(err)
		}
	}
	// count keys
	keys1 := s.Count()
	// create backup
	err = s.BackupGZ(f)
	if err != nil {
		panic(err)
	}
	err = s.Close()
	assert.NoError(t, err)

	err = DeleteStore("1")
	assert.NoError(t, err)

	s, err = Open(Dir("1"))
	assert.NoError(t, err)

	f.Seek(0, 0)
	err = s.RestoreGZ(f)
	if err != nil {
		panic(err)
	}
	keys2 := s.Count()
	assert.Equal(t, keys1, keys2)

	err = s.Close()
	assert.NoError(t, err)

	err = DeleteStore("1")
	assert.NoError(t, err)

	err = os.Remove(backup)
	assert.NoError(t, err)
}
