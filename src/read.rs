use std::io::{self, Read};
use std::fs::File;
use std::mem;
use std::path::Path;

use bytes::{BufMut, Bytes, BytesMut};
use futures::{Async, Future, Poll, Stream};
use futures_cpupool::CpuFuture;

use ::FsPool;

pub fn new<P: AsRef<Path> + Send + 'static>(pool: &FsPool, path: P) -> FsReadStream {
    let open = pool.cpu_pool.spawn_fn(move || {
        let file = try!(File::open(path));
        read(file, BytesMut::with_capacity(0))
    });
    FsReadStream {
        buffer: BytesMut::with_capacity(0),
        pool: pool.clone(),
        state: State::Working(open),
    }
}

/// A `Stream` of bytes from a target file.
pub struct FsReadStream {
    buffer: BytesMut,
    pool: FsPool,
    state: State,
}

enum State {
    Working(CpuFuture<(File, BytesMut), io::Error>),
    Ready(File),
    Eof,
    Swapping,
}

impl Stream for FsReadStream {
    type Item = Bytes;
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        let state = mem::replace(&mut self.state, State::Swapping);
        let (state, item) = match state  {
            State::Working(mut cpu) => {
                let polled = cpu.poll();
                self.state = State::Working(cpu);
                let (file, chunk) =  try_ready!(polled);
                if chunk.is_empty() {
                    (State::Eof, Async::Ready(None))
                } else {
                    self.buffer = chunk;
                    (State::Ready(file), Async::Ready(Some(self.buffer.take().freeze())))
                }
            },
            State::Ready(file) => {
                let buf = self.buffer.split_off(0);
                (State::Working(self.pool.cpu_pool.spawn_fn(move || {
                    read(file, buf)
                })), Async::NotReady)
            },
            State::Eof => (State::Eof, Async::Ready(None)),
            State::Swapping => unreachable!(),
        };
        self.state = state;
        Ok(item)
    }
}


fn read(mut file: File, mut buf: BytesMut) -> io::Result<(File, BytesMut)> {
    if !buf.has_remaining_mut() {
        buf.reserve(8192);
    }
    let n = try!(file.read(unsafe { buf.bytes_mut() }));
    unsafe { buf.advance_mut(n) };
    Ok((file, buf))
}

