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

```
MacBook Pro 2019 (Quad-Core Intel Core i7 2,8 GHz, 16 ГБ, APPLE SSD AP0512M)

go version go1.14 darwin/amd64

     number of cpus: 8
     number of keys: 1000000
            keysize: 10
        random seed: 1570109110136449000

-- sniper --
set: 1,000,000 ops over 8 threads in 2128ms, 470,034/sec, 2127 ns/op, 38.6 MB, 40 bytes/op
get: 1,000,000 ops over 8 threads in 367ms, 2,728,299/sec, 366 ns/op, 30.5 MB, 32 bytes/op
del: 1,000,000 ops over 8 threads in 769ms, 1,300,189/sec, 769 ns/op, 63.3 MB, 66 bytes/op
```

## How it is done

* Sniper database is sharded on many chunks. Each chunk has its own lock (RW), so it supports high concurrent access on multi-core CPUs.
* Each bucket consists of a `hash(key) -> (value addr, value size)`, map. It give database ability to store 100_000_000 records in ~ 20Gb of memory.
* Hash is very short, and has collisions. Sniper has resolver for that.
* Efficient space reuse alghorithm. Every packet has power of 2 size, for inplace rewrite on value update and map of deleted entrys, for reusing space.

## Limitations

* 512 Kb - entry size `len(key) + len(value)`
* 1 Tb - maximum database size

## Contact

Vadim Kulibaba [@recoilme](https://github.com/recoilme)

## License

`sniper` source code is available under the MIT [License](/LICENSE).