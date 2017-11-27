use std::{cmp, fmt, mem};
use std::fs::{self, File};
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use bytes::{BufMut, Bytes, BytesMut};
use futures::{Async, Future, Poll, Stream};
use futures_cpupool::CpuFuture;

use FsPool;

const BUF_SIZE: usize = 8192;

pub fn new<P: AsRef<Path> + Send + 'static>(pool: &FsPool, path: P) -> FsReadStream {
    FsReadStream {
        buffer: BytesMut::with_capacity(0),
        //TODO: can we adjust bounds, since this is making an owned copy anyways?
        path: Arc::new(path.as_ref().to_owned()),
        pool: pool.clone(),
        state: State::Init,
    }
}

/// A `Stream` of bytes from a target file.
pub struct FsReadStream {
    buffer: BytesMut,
    path: Arc<PathBuf>,
    pool: FsPool,
    state: State,
}

enum State {
    Init,
    Working(CpuFuture<(File, BytesMut), io::Error>),
    Ready(File),
    Eof,
    Swapping,
}

impl Stream for FsReadStream {
    type Item = Bytes;
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        loop {
            match mem::replace(&mut self.state, State::Swapping) {
                State::Init => {
                    let path = self.path.clone();
                    self.state =
                        State::Working(self.pool.cpu_pool.spawn_fn(move || open_and_read(&path)));
                }
                State::Working(mut cpu) => {
                    let polled = cpu.poll();
                    self.state = State::Working(cpu);
                    let (file, chunk) = try_ready!(polled);
                    if chunk.is_empty() {
                        self.state = State::Eof;
                        return Ok(Async::Ready(None));
                    } else {
                        self.buffer = chunk;
                        self.state = State::Ready(file);
                        return Ok(Async::Ready(Some(self.buffer.take().freeze())));
                    }
                }
                State::Ready(file) => {
                    let buf = self.buffer.split_off(0);
                    self.state =
                        State::Working(self.pool.cpu_pool.spawn_fn(move || read(file, buf)));
                }
                State::Eof => {
                    self.state = State::Eof;
                    return Ok(Async::Ready(None));
                }
                State::Swapping => unreachable!(),
            }
        }
    }
}

impl fmt::Debug for FsReadStream {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("FsReadStream")
            .field("path", &self.path)
            .finish()
    }
}

fn read(mut file: File, mut buf: BytesMut) -> io::Result<(File, BytesMut)> {
    if !buf.has_remaining_mut() {
        buf.reserve(BUF_SIZE);
    }
    let n = file.read(unsafe { buf.bytes_mut() })?;
    unsafe { buf.advance_mut(n) };
    Ok((file, buf))
}

fn open_and_read(path: &Path) -> io::Result<(File, BytesMut)> {
    let len = fs::metadata(path)?.len();
    let file = File::open(path)?;

    // if size is smaller than our chunk size, dont reserve wasted space
    let initial_cap = cmp::min(len as usize, BUF_SIZE);
    read(file, BytesMut::with_capacity(initial_cap))
}
