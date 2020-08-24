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
s, _ := sniper.Open(sniper.Dir("1"))
s.Set([]byte("hello"), []byte("go"))
res, _ = s.Get([]byte("hello"))
fmt.Println(res)
s.Close()
// Output:
// go
```

## Performance

```
MacBook Pro 2019 (Quad-Core Intel Core i7 2,8 GHz, 16 ГБ, APPLE SSD AP0512M)

go version go1.14 darwin/amd64

     number of cpus: 8
     number of keys: 10000000
            keysize: 10
        random seed: 1570109110136449000

-- sniper --
set: 10,000,000 ops over 8 threads in 63159ms, 158,331/sec, 6315 ns/op, 644.3 MB, 67 bytes/op
get: 10,000,000 ops over 8 threads in 4455ms, 2,244,629/sec, 445 ns/op, 305.5 MB, 32 bytes/op
del: 10,000,000 ops over 8 threads in 37568ms, 266,182/sec, 3756 ns/op, 122.8 MB, 12 bytes/op

With fsync

set: 10,000,000 ops over 8 threads in 85088ms, 117,524/sec, 8508 ns/op, 644.4 MB, 67 bytes/op
get: 10,000,000 ops over 8 threads in 5623ms, 1,778,268/sec, 562 ns/op, 305.5 MB, 32 bytes/op

With tree
set: 10,000,000 ops over 8 threads in 35663ms, 280,399/sec, 3566 ns/op, 964.1 MB, 101 bytes/op
set: 10,000,000 ops over 8 threads in 55610ms, 179,824/sec, 5560 ns/op, 1.4 GB, 150 bytes/op
set: 10,000,000 ops over 8 threads in 36697ms, 272,503/sec, 3669 ns/op, 922.0 MB, 96 bytes/op
```

## How it is done

* Sniper database is sharded on 250+ chunks. Each chunk has its own lock (RW), so it supports high concurrent access on multi-core CPUs.
* Each chunk store `hash(key) -> (value addr, value size)`, map. 
* Hash is very short, and has collisions. Sniper has collisions resolver.
* Efficient space reuse alghorithm. Every packet has power of 2 size, for inplace rewrite on value update and map of deleted entrys, for reusing space.

## Limitations

* 512 Kb - maximum entry size `len(key) + len(value)`
* ~1 Tb - maximum database size

## Mac OS tip

[How to Change Open Files Limit on OS X and macOS](https://gist.github.com/tombigel/d503800a282fcadbee14b537735d202c)

## Contact

Vadim Kulibaba [@recoilme](https://github.com/recoilme)

## License

`sniper` source code is available under the MIT [License](/LICENSE).