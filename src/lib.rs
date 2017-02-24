extern crate bytes;
#[macro_use]
extern crate futures;
extern crate futures_cpupool;

use std::io;
use std::fs;
use std::path::Path;

use futures::{Async, Future, Stream, Poll};
use futures_cpupool::{CpuPool, CpuFuture};

pub use self::read::FsReadStream;
pub use self::write::FsWriteSink;
pub use self::write::WriteOptions;

mod read;
mod write;

#[derive(Clone)]
pub struct FsPool {
    cpu_pool: CpuPool,
}

impl FsPool {
    pub fn new(threads: usize) -> FsPool {
        FsPool {
            cpu_pool: CpuPool::new(threads),
        }
    }

    pub fn read<P: AsRef<Path> + Send + 'static>(&self, path: P) -> FsReadStream {
        ::read::new(self, path)
    }

    pub fn write<P: AsRef<Path> + Send + 'static>(&self, path: P, opts: WriteOptions) -> FsWriteSink {
        ::write::new(self, path, opts)
    }

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
