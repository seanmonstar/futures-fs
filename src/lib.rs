#![deny(missing_docs)]
#![deny(warnings)]
#![deny(missing_debug_implementations)]
#![doc(html_root_url = "https://docs.rs/futures-fs/0.0.3")]

//! A thread pool to handle file IO operations.
//!
//! # Examples
//!
//! ```rust
//! extern crate futures;
//! extern crate futures_fs;
//!
//! use futures::{Future, Stream};
//! use futures_fs::FsPool;
//!
//! # fn run() {
//! let fs = FsPool::default();
//!
//! // our source file
//! let read = fs.read("/home/sean/foo.txt", Default::default());
//!
//! // default writes options to create a new file
//! let write = fs.write("/home/sean/out.txt", Default::default());
//!
//! // block this thread!
//! // the reading and writing however will happen off-thread
//! read.forward(write).wait()
//!     .expect("IO error piping foo.txt to out.txt");
//! # }
//! # fn main() {}
//! ```

extern crate bytes;
#[macro_use]
extern crate futures;
extern crate futures_cpupool;

use std::{fmt, fs, io};
use std::path::Path;

use futures::{Future, Poll};
use futures_cpupool::{CpuFuture, CpuPool};

pub use self::read::{FsReadStream, ReadOptions};
pub use self::write::{FsWriteSink, WriteOptions};

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
    pub fn read<P: AsRef<Path> + Send + 'static>(
        &self,
        path: P,
        opts: ReadOptions,
    ) -> FsReadStream {
        ::read::new(self, path, opts)
    }

    /// Returns a `Sink` to send bytes to be written to the file at the supplied path.
    pub fn write<P: AsRef<Path> + Send + 'static>(
        &self,
        path: P,
        opts: WriteOptions,
    ) -> FsWriteSink {
        ::write::new(self, path, opts)
    }

    /// Returns a `Future` that resolves when the target file is deleted.
    pub fn delete<P: AsRef<Path> + Send + 'static>(&self, path: P) -> FsFuture<()> {
        fs(self.cpu_pool.spawn_fn(move || fs::remove_file(path)))
    }
}

impl Default for FsPool {
    fn default() -> FsPool {
        FsPool::new(4)
    }
}

impl fmt::Debug for FsPool {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("FsPool").finish()
    }
}

/// A future representing work in the `FsPool`.
pub struct FsFuture<T> {
    inner: CpuFuture<T, io::Error>,
}

fn fs<T: Send>(cpu: CpuFuture<T, io::Error>) -> FsFuture<T> {
    FsFuture { inner: cpu }
}

impl<T: Send + 'static> Future for FsFuture<T> {
    type Item = T;
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        self.inner.poll()
    }
}

impl<T> fmt::Debug for FsFuture<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("FsFuture").finish()
    }
}
