use std::io::{self, Read};
use std::fs::File;
use std::mem;
use std::path::Path;

use futures::{Async, Future, Poll, Stream};
use futures_cpupool::CpuFuture;

use ::FsPool;

pub fn new<P: AsRef<Path> + Send + 'static>(pool: &FsPool, path: P) -> FsReadStream {
    let open = pool.cpu_pool.spawn_fn(move || {
        let file = try!(File::open(path));
        read(file)
    });
    FsReadStream {
        pool: pool.clone(),
        state: State::Working(open),
    }
}

type Chunk = Vec<u8>;

pub struct FsReadStream {
    pool: FsPool,
    state: State,
}

enum State {
    Working(CpuFuture<(File, Chunk), io::Error>),
    Ready(File),
    Eof,
    Swapping,
}

impl Stream for FsReadStream {
    type Item = Vec<u8>;
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
                    (State::Ready(file), Async::Ready(Some(chunk)))
                }
            },
            State::Ready(file) => {
                (State::Working(self.pool.cpu_pool.spawn_fn(move || {
                    read(file)
                })), Async::NotReady)
            },
            State::Eof => (State::Eof, Async::Ready(None)),
            State::Swapping => unreachable!(),
        };
        self.state = state;
        Ok(item)
    }
}


fn read(mut file: File) -> io::Result<(File, Chunk)> {
    let mut chunk = vec![0; 4096];
    let n = try!(file.read(&mut chunk));
    chunk.truncate(n);
    Ok((file, chunk))
}

