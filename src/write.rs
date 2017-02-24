use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::path::Path;

use futures::{Async, AsyncSink, Future, Poll, Sink, StartSend};
use futures_cpupool::CpuFuture;

use ::FsPool;

pub fn new<P: AsRef<Path> + Send + 'static>(pool: &FsPool, path: P, opts: WriteOptions) -> FsWriteSink {
    let open = pool.cpu_pool.spawn_fn(move || {
        opts.open.open(path)
    });
    FsWriteSink {
        pool: pool.clone(),
        state: State::Working(open),
    }
}

pub struct FsWriteSink {
    pool: FsPool,
    state: State,
}

#[derive(Debug)]
pub struct WriteOptions {
    open: OpenOptions,
}

impl Default for WriteOptions {
    fn default() -> WriteOptions {
        WriteOptions {
            open: OpenOptions::new()
                .write(true)
                .create(true),
        }
    }
}

impl From<OpenOptions> for WriteOptions {
    fn from(open: OpenOptions) -> WriteOptions {
        WriteOptions {
            open: open,
        }
    }
}

enum State {
    Working(CpuFuture<File, io::Error>),
    Ready(File),
    Swapping,
}

impl FsWriteSink {
    fn poll_working(&mut self) -> Poll<(), io::Error> {
        let state = match self.state {
            State::Working(ref mut cpu) => {
                let file = try_ready!(cpu.poll());
                State::Ready(file)
            }
            State::Ready(_) => {
                return Ok(Async::Ready(()));
            },
            State::Swapping => unreachable!(),
        };
        self.state = state;
        Ok(Async::Ready(()))
    }
}

impl Sink for FsWriteSink {
    type SinkItem = &'static str;
    type SinkError = io::Error;

    fn start_send(&mut self, item: Self::SinkItem) -> StartSend<Self::SinkItem, Self::SinkError> {
        let state = try!(self.poll_working());
        if state.is_ready() {
            let mut file = match ::std::mem::replace(&mut self.state, State::Swapping) {
                State::Ready(file) => file,
                _ => unreachable!(),
            };
            self.state = State::Working(self.pool.cpu_pool.spawn_fn(move || {
                try!(file.write_all(item.as_ref()));
                Ok(file)
            }));
            Ok(AsyncSink::Ready)
        } else {
            Ok(AsyncSink::NotReady(item))
        }
    }

    fn poll_complete(&mut self) -> Poll<(), Self::SinkError> {
        self.poll_working()
    }
}
