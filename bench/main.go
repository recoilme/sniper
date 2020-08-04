package main

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"time"

	"github.com/recoilme/sniper"
	"github.com/tidwall/lotsa"
)

func randKey(rnd *rand.Rand, n int) []byte {
	s := make([]byte, n)
	rnd.Read(s)
	for i := 0; i < n; i++ {
		s[i] = 'a' + (s[i] % 26)
	}
	return s
}

func seed() ([][]byte, int) {
	seed := int64(1570109110136449000) //time.Now().UnixNano() //1570108152262917000
	// println(seed)
	rng := rand.New(rand.NewSource(seed))
	N := 10_000_000
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

	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	fmt.Printf("Alloc = %v MiB Total = %v MiB\n", (ms.Alloc / 1024 / 1024), (ms.TotalAlloc / 1024 / 1024))

	fmt.Println("-- sniper --")
	sniper.DeleteStore("1")
	s, err := sniper.Open(sniper.Dir("1"), sniper.SyncInterval(1*time.Second))
	if err != nil {
		panic(err)
	}
	fmt.Print("set: ")
	coll := 0
	lotsa.Ops(N, runtime.NumCPU(), func(i, _ int) {
		b := make([]byte, 8)
		binary.BigEndian.PutUint64(b, uint64(i))
		//println("set", i, keys[i], b)
		err := s.Set(keys[i], b)
		if err == sniper.ErrCollision {
			coll++
			err = nil
		}
		if err != nil {
			panic(err)
		}
	})

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

	var ms2 runtime.MemStats
	runtime.ReadMemStats(&ms2)

	fmt.Printf("Alloc = %v MiB Total = %v MiB\n", (ms2.Alloc / 1024 / 1024), (ms2.TotalAlloc / 1024 / 1024))

	fmt.Print("del: ")
	lotsa.Ops(N, runtime.NumCPU(), func(i, _ int) {
		s.Delete(keys[i])
	})
	err = sniper.DeleteStore("1")
	fmt.Println(err)
}

func main() {
	keys, N := seed()

	sniperBench(keys, N)

	//uncomment for badger test

	//budgerBench(keys, N)
}

/*
func budgerBench(keys [][]byte, N int) {
	sniper.DeleteStore("badger_test")
	bd, err := newBadgerdb("badger_test")
	if err != nil {
		panic(err)
	}
	println("-- badger --")
	print("set: ")

	lotsa.Ops(N, runtime.NumCPU(), func(i, _ int) {
		txn := bd.NewTransaction(true) // Read-write txn
		b := make([]byte, 8)
		binary.BigEndian.PutUint64(b, uint64(i))

		err = txn.SetEntry(badger.NewEntry(keys[i], b))
		if err != nil {
			log.Fatal(err)
		}
		err = txn.Commit()
		if err != nil {
			log.Fatal(err)
		}

	})

	print("get: ")
	lotsa.Ops(N, runtime.NumCPU(), func(i, _ int) {
		var val []byte
		err := bd.View(func(txn *badger.Txn) error {
			item, err := txn.Get(keys[i])
			if err != nil {
				return err
			}
			val, err = item.ValueCopy(val)
			return err
		})
		if err != nil {
			log.Fatal(err)
		}
		v := binary.BigEndian.Uint64(val)
		if uint64(i) != v {
			panic("bad news")
		}
	})

	print("del: ")
	lotsa.Ops(N, runtime.NumCPU(), func(i, _ int) {
		txn := bd.NewTransaction(true)
		err := txn.Delete(keys[i])
		if err != nil {
			log.Fatal(err)
		}
		err = txn.Commit()
		if err != nil {
			log.Fatal(err)
		}
	})

	sniper.DeleteStore("badger_test")

}
func newBadgerdb(path string) (*badger.DB, error) {

	os.MkdirAll(path, os.FileMode(0777))
	opts := badger.DefaultOptions(path)
	opts.SyncWrites = false
	opts.Logger = nil
	return badger.Open(opts)
}
*/
