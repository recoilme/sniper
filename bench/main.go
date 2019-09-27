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

func main() {

	seed := time.Now().UnixNano()
	// println(seed)
	rng := rand.New(rand.NewSource(seed))
	N := 1_000_000
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

	lotsa.Output = os.Stdout
	lotsa.MemUsage = true

	println("-- sniper --")
	sniper.DeleteStore("1")
	s, _ := sniper.Open("1")
	print("set: ")
	lotsa.Ops(N, runtime.NumCPU(), func(i, _ int) {
		b := make([]byte, 8)
		binary.BigEndian.PutUint64(b, uint64(i))
		s.Set(keys[i], b)
	})
	print("get: ")
	lotsa.Ops(N, runtime.NumCPU(), func(i, _ int) {
		b, err := s.Get(keys[i])
		if err != nil {
			panic(err)
		}
		v := binary.BigEndian.Uint64(b)

		if uint64(i) != v {
			panic("bad news")
		}
	})

	print("del: ")
	lotsa.Ops(N, runtime.NumCPU(), func(i, _ int) {
		s.Delete(keys[i])
	})
	sniper.DeleteStore("1")
	println()

}
