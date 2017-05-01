//! A CPU Pool to handle file IO operations.
#![deny(missing_docs)]
#![deny(warnings)]

extern crate bytes;
#[macro_use]
extern crate futures;
extern crate futures_cpupool;

use std::io;
use std::fs;
use std::path::Path;

use futures::{Future, Poll};
use futures_cpupool::{CpuPool, CpuFuture};

pub use self::read::FsReadStream;
pub use self::write::FsWriteSink;
pub use self::write::WriteOptions;

mod read;
mod write;

/// A pool of threads to handle file IO.
#[derive(Clone)]
pub struct FsPool {
    cpu_pool: CpuPool,
}

impl FsPool {
    /// Creates a new `FsPool`, with the supplied number of threads.
    pub fn new(threads: usize) -> FsPool {
        FsPool {
            cpu_pool: CpuPool::new(threads),
        }
    }

    /// Returns a `Stream` of the contents of the file at the supplied path.
    pub fn read<P: AsRef<Path> + Send + 'static>(&self, path: P) -> FsReadStream {
        ::read::new(self, path)
    }

    /// Returns a `Sink` to send bytes to be written to the file at the supplied path.
    pub fn write<P: AsRef<Path> + Send + 'static>(&self, path: P, opts: WriteOptions) -> FsWriteSink {
        ::write::new(self, path, opts)
    }

    /// Returns a `Future` that resolves when the target file is deleted.
    pub fn delete<P: AsRef<Path> + Send + 'static>(&self, path: P) -> FsFuture<()> {
        fs(self.cpu_pool.spawn_fn(move || {
            fs::remove_file(path)
        }))
    }
}

impl Default for FsPool {
    fn default() -> FsPool {
        FsPool::new(4)
    }
}

/// A future representing work in the `FsPool`.
pub struct FsFuture<T> {
    inner: CpuFuture<T, io::Error>,
}

fn fs<T: Send>(cpu: CpuFuture<T, io::Error>) -> FsFuture<T> {
    FsFuture {
        inner: cpu,
    }
}

impl<T: Send + 'static> Future for FsFuture<T> {
    type Item = T;
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        self.inner.poll()
    }
}
