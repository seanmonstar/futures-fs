use std::cmp;
use std::fs::{self, File};
use std::io::{self, Read};
use std::mem;
use std::path::{PathBuf, Path};

use bytes::{BufMut, Bytes, BytesMut};
use futures::{Async, Future, Poll, Stream};
use futures_cpupool::CpuFuture;

use ::FsPool;

const BUF_SIZE: usize = 8192;

pub fn new<P: AsRef<Path> + Send + 'static>(pool: &FsPool, path: P) -> FsReadStream {
    FsReadStream {
        buffer: BytesMut::with_capacity(0),
        pool: pool.clone(),
        state: State::Init(path.as_ref().to_owned()), //State::Working(open),
    }
}

/// A `Stream` of bytes from a target file.
pub struct FsReadStream {
    buffer: BytesMut,
    pool: FsPool,
    state: State,
}

enum State {
    Init(PathBuf),
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
                State::Init(path) => {
                    self.state = State::Working(self.pool.cpu_pool.spawn_fn(move || {
                        open_and_read(path)
                    }));
                },
                State::Working(mut cpu) => {
                    let polled = cpu.poll();
                    self.state = State::Working(cpu);
                    let (file, chunk) =  try_ready!(polled);
                    if chunk.is_empty() {
                        self.state = State::Eof;
                        return Ok(Async::Ready(None));
                    } else {
                        self.buffer = chunk;
                        self.state = State::Ready(file);
                        return Ok(Async::Ready(Some(self.buffer.take().freeze())));
                    }
                },
                State::Ready(file) => {
                    let buf = self.buffer.split_off(0);
                    self.state = State::Working(self.pool.cpu_pool.spawn_fn(move || {
                        read(file, buf)
                    }));
                },
                State::Eof => {
                    self.state = State::Eof;
                    return Ok(Async::Ready(None));
                },
                State::Swapping => unreachable!(),
            }
        }
    }
}


fn read(mut file: File, mut buf: BytesMut) -> io::Result<(File, BytesMut)> {
    if !buf.has_remaining_mut() {
        buf.reserve(BUF_SIZE);
    }
    let n = try!(file.read(unsafe { buf.bytes_mut() }));
    unsafe { buf.advance_mut(n) };
    Ok((file, buf))
}

fn open_and_read(path: PathBuf) -> io::Result<(File, BytesMut)> {
    let len = try!(fs::metadata(&path)).len();
    let file = try!(File::open(path));

    // if size is smaller than our chunk size, dont reserve wasted space
    let initial_cap = cmp::min(len as usize, BUF_SIZE);
    read(file, BytesMut::with_capacity(initial_cap))
}
