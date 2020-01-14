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