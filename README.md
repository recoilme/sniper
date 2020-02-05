# `sniper`

[![GoDoc](https://img.shields.io/badge/api-reference-blue.svg?style=flat-square)](https://godoc.org/github.com/recoilme/sniper)

A simple and efficient thread-safe key/value store for Go.


# Getting Started

## Features

* Store hundreds of millions of entries
* Fast. High concurrent. Thread-safe. Scales on multi-core CPUs
* Extremly low memory usage
* Zero GC overhead
* Simple, pure Go implementation

## Installing

To start using `sniper`, install Go and run `go get`:

```sh
$ go get -u github.com/recoilme/sniper
```

This will retrieve the library.

## Usage

The `Sniper` includes this methods:
`Set`, `Get`, `Incr`, `Decr`, `Delete`, `Count`, `Open`, `Close`, `FileSize`, `Backup`.

```go
s, _ := sniper.Open("1")
s.Set([]byte("hello"), []byte("go"))
res, _ = s.Get([]byte("hello"))
fmt.Println(res)
s.Close()
// Output:
// go
```

## Performance

Benchmarking conncurrent SET, GET, DELETE operations vs github.com/dgraph-io/badger v1.6.0

**MacBook Pro, 2019**
```
go version go1.13.6 darwin/amd64

     number of cpus: 8
     number of keys: 20_000_000
            keysize: 10
        random seed: 1570109110136449000

-- sniper --
set: 20,000,000 ops over 8 threads in 49968ms, 400,258/sec, 2498 ns/op, 1.3 GB, 67 bytes/op
get: 20,000,000 ops over 8 threads in 8492ms, 2,355,030/sec, 424 ns/op, 611.8 MB, 32 bytes/op
del: 20,000,000 ops over 8 threads in 38364ms, 521,317/sec, 1918 ns/op, 1.1 GB, 59 bytes/op
Size on disk: 640 000 000 byte

-- badger --
set: 20,000,000 ops over 8 threads in 200468ms, 99,766/sec, 10023 ns/op, 703.6 MB, 36 bytes/op
get: 20,000,000 ops over 8 threads in 42823ms, 467,042/sec, 2141 ns/op, 852.9 MB, 44 bytes/op
del: 20,000,000 ops over 8 threads in 201823ms, 99,096/sec, 10091 ns/op, 2.0 GB, 106 bytes/op

Size on disk: 4 745 317 924 byte


number of keys: 100_000_000:
-- sniper --
set: 100,000,000 ops over 8 threads in 350252ms, 285,508/sec, 3502 ns/op, 3.0 GB, 32 bytes/op
get: 100,000,000 ops over 8 threads in 48400ms, 2,066,111/sec, 484 ns/op, 3.0 GB, 32 bytes/op
del: 100,000,000 ops over 8 threads in 200237ms, 499,408/sec, 2002 ns/op, 2.5 GB, 27 bytes/op

-- badger --
no results (after 2 hours, 23+ Gb on disk)
```

**[kvbench](https://github.com/recoilme/kvbench)**


nofsync - throughputs

| |sniper|badger|bbolt|bolt|leveldb|kv|buntdb|pebble|rocksdb|btree|map|map/memory|
|--|--|--|--|--|--|--|--|--|--|--|--|--|
|set|373915|111035|27424|27812|261191|15499|136580|181822|179716|160401|185961|1362789|
|get|2015105|768845|900809|994350|845286|66374|4427641|624495|630244|7993960|13640102|12237355|
|setmixed|53077|24459|15133|16818|30914|5749|34848|84642|82205|66104|108259|122343|
|getmixed|1665159|291475|695127|737203|346165|46000|293039|402267|404589|529255|870928|1049823|
|del|7285803|116208|14845|16223|192040|819370|412601|274388|268528|1000032|1364012|6200300|



nofsync - time

| |sniper|badger|bbolt|bolt|leveldb|kv|buntdb|pebble|rocksdb|btree|map|map/memory|
|--|--|--|--|--|--|--|--|--|--|--|--|--|
|set|334|1125|4557|4494|478|8065|915|687|695|779|672|91|
|get|62|162|138|125|147|1883|28|200|198|15|9|10|
|setmixed|18840|40883|66080|59456|32347|173913|28695|11814|12164|15127|9237|8173|
|getmixed|75|428|179|169|361|2717|426|310|308|236|143|119|
|del|17|1075|8419|7704|650|152|302|455|465|124|91|20|


**redis-benchmark (no pipelining)**

```
====== PING_INLINE ======
  100000 requests completed in 0.74 seconds
  50 parallel clients
  3 bytes payload
  keep alive: 1

100.00% <= 0 milliseconds
136054.42 requests per second

====== PING_BULK ======
  100000 requests completed in 0.73 seconds
  50 parallel clients
  3 bytes payload
  keep alive: 1

100.00% <= 0 milliseconds
137362.64 requests per second

====== SET ======
  100000 requests completed in 1.06 seconds
  50 parallel clients
  3 bytes payload
  keep alive: 1

68.10% <= 1 milliseconds
99.06% <= 2 milliseconds
99.96% <= 3 milliseconds
100.00% <= 3 milliseconds
94607.38 requests per second

====== GET ======
  100000 requests completed in 0.76 seconds
  50 parallel clients
  3 bytes payload
  keep alive: 1

100.00% <= 0 milliseconds
130890.05 requests per second
```

redis-benchmark -P 100 (pipelining)

```
====== PING_INLINE ======
  100000 requests completed in 0.04 seconds
  50 parallel clients
  3 bytes payload
  keep alive: 1

73.90% <= 1 milliseconds
99.30% <= 2 milliseconds
100.00% <= 2 milliseconds
2777778.00 requests per second

====== PING_BULK ======
  100000 requests completed in 0.03 seconds
  50 parallel clients
  3 bytes payload
  keep alive: 1

77.90% <= 1 milliseconds
100.00% <= 1 milliseconds
3030303.00 requests per second

====== SET ======
  100000 requests completed in 0.37 seconds
  50 parallel clients
  3 bytes payload
  keep alive: 1

0.00% <= 1 milliseconds
0.40% <= 2 milliseconds
1.30% <= 3 milliseconds
2.80% <= 4 milliseconds
6.70% <= 5 milliseconds
10.20% <= 6 milliseconds
14.00% <= 7 milliseconds
18.40% <= 8 milliseconds
23.10% <= 9 milliseconds
28.00% <= 10 milliseconds
32.60% <= 11 milliseconds
36.90% <= 12 milliseconds
41.25% <= 13 milliseconds
45.95% <= 14 milliseconds
50.55% <= 15 milliseconds
54.64% <= 16 milliseconds
57.74% <= 17 milliseconds
61.83% <= 18 milliseconds
65.73% <= 19 milliseconds
70.13% <= 20 milliseconds
74.13% <= 21 milliseconds
77.51% <= 22 milliseconds
80.51% <= 23 milliseconds
82.81% <= 24 milliseconds
85.01% <= 25 milliseconds
87.11% <= 26 milliseconds
89.01% <= 27 milliseconds
90.71% <= 28 milliseconds
91.91% <= 29 milliseconds
93.41% <= 30 milliseconds
94.21% <= 31 milliseconds
94.91% <= 32 milliseconds
95.41% <= 33 milliseconds
96.30% <= 34 milliseconds
97.00% <= 35 milliseconds
97.70% <= 36 milliseconds
98.10% <= 37 milliseconds
98.30% <= 38 milliseconds
98.40% <= 39 milliseconds
98.90% <= 41 milliseconds
99.20% <= 42 milliseconds
99.30% <= 43 milliseconds
99.40% <= 44 milliseconds
99.50% <= 45 milliseconds
99.60% <= 46 milliseconds
99.70% <= 48 milliseconds
99.80% <= 50 milliseconds
99.90% <= 52 milliseconds
100.00% <= 52 milliseconds
268817.19 requests per second

====== GET ======
  100000 requests completed in 0.04 seconds
  50 parallel clients
  3 bytes payload
  keep alive: 1

69.60% <= 1 milliseconds
98.10% <= 2 milliseconds
100.00% <= 2 milliseconds
2702702.75 requests per second
```

**Macbook Early 2015**
Benchmarking conncurrent SET, GET, DELETE operations vs github.com/dgraph-io/badger v1.6.0

```
go version go1.13 darwin/amd64 (Macbook Early 2015)

     number of cpus: 4
     number of keys: 1000000
            keysize: 10
        random seed: 1569597566903802000

-- sniper --

set: 1,000,000 ops over 4 threads in 4159ms, 240,455/sec, 4158 ns/op, 57.7 MB, 60 bytes/op
get: 1,000,000 ops over 4 threads in 1988ms, 502,997/sec, 1988 ns/op, 30.5 MB, 32 bytes/op
del: 1,000,000 ops over 4 threads in 4430ms, 225,729/sec, 4430 ns/op, 29.0 MB, 30 bytes/op

-- badger --

set: 1,000,000 ops over 4 threads in 25331ms, 39,476/sec, 25331 ns/op, 121.0 MB, 126 bytes/op
get: 1,000,000 ops over 4 threads in 2222ms, 450,007/sec, 2222 ns/op, 53.9 MB, 56 bytes/op
del: 1,000,000 ops over 4 threads in 25292ms, 39,538/sec, 25291 ns/op, 42.2 MB, 44 bytes/op

```


## How it is done

* Sniper database is sharded on many chunks. Each chunk has its own lock (RW), so it supports high concurrent access on multi-core CPUs.
* Each bucket consists of a `hash(key) -> (value addr, value size)`, map[uint32]uint32. Tricky encoded (we use every bit) in just 4 bytes. It give database ability to store 100_000_000 of keys in 2Gb of memory.
* Hash is very short, and has collisions. Sniper has resolver for that (some special chunks).
* Efficient space reuse alghorithm. Every packet has power of 2 size, for inplace rewrite on value update and map of deleted entrys, for reusing space.

## Limitations

* 512 Kb - entry size `len(key) + len(value)`
* 64 Gb - maximum database size
* 8 byte - header size for every entry in file

## Contact

Vadim Kulibaba [@recoilme](https://github.com/recoilme)

## License

`sniper` source code is available under the MIT [License](/LICENSE).