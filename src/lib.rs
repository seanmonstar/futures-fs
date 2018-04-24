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
use std::sync::Arc;

use futures::{Async, Future, Poll};
use futures::future::{lazy, Executor};
use futures::sync::oneshot::{self, Receiver};
use futures_cpupool::CpuPool;

pub use self::read::{FsReadStream, ReadOptions};
pub use self::write::{FsWriteSink, WriteOptions};

mod read;
mod write;

/// A pool of threads to handle file IO.
#[derive(Clone)]
pub struct FsPool {
    executor: Arc<Executor<Box<Future<Item = (), Error = ()> + Send>>>,
}

impl FsPool {
    /// Creates a new `FsPool`, with the supplied number of threads.
    pub fn new(threads: usize) -> Self {
        FsPool {
            executor: Arc::new(CpuPool::new(threads)),
        }
    }

    /// Creates a new `FsPool`, from an existing `Executor`.
    pub fn from_executor<E>(executor: E) -> Self
    where
        E: Executor<Box<Future<Item = (), Error = ()> + Send>> + Clone + 'static,
    {
        FsPool {
            executor: Arc::new(executor),
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

    /// Returns a `Stream` of the contents of the supplied file.
    pub fn read_file(&self, file: fs::File, opts: ReadOptions) -> FsReadStream {
        ::read::new_from_file(self, file, opts)
    }

    /// Returns a `Sink` to send bytes to be written to the file at the supplied path.
    pub fn write<P: AsRef<Path> + Send + 'static>(
        &self,
        path: P,
        opts: WriteOptions,
    ) -> FsWriteSink {
        ::write::new(self, path, opts)
    }

    /// Returns a `Sink` to send bytes to be written to the supplied file.
    pub fn write_file(&self, file: fs::File) -> FsWriteSink {
        ::write::new_from_file(self, file)
    }

    /// Returns a `Future` that resolves when the target file is deleted.
    pub fn delete<P: AsRef<Path> + Send + 'static>(&self, path: P) -> FsFuture<()> {
        let (tx, rx) = oneshot::channel();

        let fut = Box::new(lazy(move || {
            tx.send(fs::remove_file(path).map_err(From::from))
                .map_err(|_| ())
        }));

        self.executor.execute(fut).unwrap();

        fs(rx)
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
    inner: Receiver<io::Result<T>>,
}

fn fs<T: Send>(rx: Receiver<io::Result<T>>) -> FsFuture<T> {
    FsFuture { inner: rx }
}

impl<T: Send + 'static> Future for FsFuture<T> {
    type Item = T;
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match self.inner.poll().unwrap() {
            Async::Ready(Ok(item)) => Ok(Async::Ready(item)),
            Async::Ready(Err(e)) => Err(e),
            Async::NotReady => Ok(Async::NotReady),
        }
    }
}

impl<T> fmt::Debug for FsFuture<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("FsFuture").finish()
    }
}
