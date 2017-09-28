# futures-fs

[![Travis Build Status](https://travis-ci.org/seanmonstar/futures-fs.svg?branch=master)](https://travis-ci.org/seanmonstar/futures-fs)
[![crates.io](https://img.shields.io/crates/v/futures-fs.svg)](https://crates.io/crates/futures-fs)

Access File System operations off-thread, using `Future`s and `Stream`s.

- [Documentation](https://docs.rs/futures-fs)

## Usage

```rust
let fs = FsPool::default();

// our source file
let read = fs.read("/home/sean/foo.txt");

// default writes options to create a new file
let write = fs.write("/home/sean/out.txt", Default::default());

// block this thread!
// the reading and writing however will happen off-thread
read.forward(write).wait()
    .expect("IO error piping foo.txt to out.txt");
```
