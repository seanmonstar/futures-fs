use std::fmt;
use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::path::Path;

use bytes::Bytes;
use futures::{Async, AsyncSink, Future, Poll, Sink, StartSend};
use futures::future::lazy;
use futures::sync::oneshot;

use FsPool;
use FsFuture;

pub fn new<P>(pool: &FsPool, path: P, opts: WriteOptions) -> FsWriteSink
where
    P: AsRef<Path> + Send + 'static,
{
    let (tx, rx) = oneshot::channel();

    let fut = Box::new(lazy(move || {
        let res = opts.open.open(path).map_err(From::from);

        tx.send(res).map_err(|_| ())
    }));

    pool.executor.execute(fut).unwrap();

    FsWriteSink {
        pool: pool.clone(),
        state: State::Working(super::fs(rx)),
    }
}

pub fn new_from_file(pool: &FsPool, file: File) -> FsWriteSink {
    FsWriteSink {
        pool: pool.clone(),
        state: State::Ready(file),
    }
}

/// A `Sink` to send bytes to be written to a target file.
pub struct FsWriteSink {
    pool: FsPool,
    state: State,
}

/// Options for how to write to the target file.
///
/// The default is to create a new file at the path.
///
/// This can be created from `std::fs::OpenOptions`.
#[derive(Debug)]
pub struct WriteOptions {
    open: OpenOptions,
}

impl Default for WriteOptions {
    fn default() -> WriteOptions {
        let mut opts = OpenOptions::new();
        opts.write(true).create(true);
        WriteOptions { open: opts }
    }
}

impl From<OpenOptions> for WriteOptions {
    fn from(open: OpenOptions) -> WriteOptions {
        WriteOptions { open: open }
    }
}

enum State {
    Working(FsFuture<File>),
    Ready(File),
    Swapping,
}

impl FsWriteSink {
    fn poll_working(&mut self) -> Poll<(), io::Error> {
        let state = match self.state {
            State::Working(ref mut rx) => {
                let file = try_ready!(rx.poll());
                State::Ready(file)
            }
            State::Ready(_) => {
                return Ok(Async::Ready(()));
            }
            State::Swapping => unreachable!(),
        };
        self.state = state;
        Ok(Async::Ready(()))
    }
}

impl Sink for FsWriteSink {
    type SinkItem = Bytes;
    type SinkError = io::Error;

    fn start_send(&mut self, item: Self::SinkItem) -> StartSend<Self::SinkItem, Self::SinkError> {
        let state = self.poll_working()?;
        if state.is_ready() {
            let mut file = match ::std::mem::replace(&mut self.state, State::Swapping) {
                State::Ready(file) => file,
                _ => unreachable!(),
            };

            let (tx, rx) = oneshot::channel();

            let fut = Box::new(lazy(move || {
                let res = file.write_all(item.as_ref())
                    .map(|_| file)
                    .map_err(From::from);

                tx.send(res).map_err(|_| ())
            }));

            self.pool.executor.execute(fut).unwrap();

            self.state = State::Working(super::fs(rx));
            Ok(AsyncSink::Ready)
        } else {
            Ok(AsyncSink::NotReady(item))
        }
    }

    fn poll_complete(&mut self) -> Poll<(), Self::SinkError> {
        self.poll_working()
    }
}

impl fmt::Debug for FsWriteSink {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("FsWriteSink").finish()
    }
}
