use std::{cmp, fmt, mem};
use std::fs::{self, File, Metadata};
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use bytes::{BufMut, Bytes, BytesMut};
use futures::{Async, Future, Poll, Stream};
use futures_cpupool::CpuFuture;

use FsPool;

const BUF_SIZE: usize = 8192;

/// Options for how to read the file.
///
/// The default is to automatically determine the buffer size.
#[derive(Debug)]
pub struct ReadOptions {
    /// The buffer size to use.
    ///
    /// If set to `None`, this is automatically determined from the operating system.
    buffer_size: Option<usize>,
}

impl Default for ReadOptions {
    fn default() -> ReadOptions {
        ReadOptions { buffer_size: None }
    }
}

pub fn new<P: AsRef<Path> + Send + 'static>(
    pool: &FsPool,
    path: P,
    opts: ReadOptions,
) -> FsReadStream {
    FsReadStream {
        buffer: BytesMut::with_capacity(0),
        //TODO: can we adjust bounds, since this is making an owned copy anyways?
        path: Arc::new(path.as_ref().to_owned()),
        pool: pool.clone(),
        state: State::Init(opts.buffer_size),
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
    Init(Option<usize>),
    Opening(CpuFuture<(File, BytesMut), io::Error>),
    Working(CpuFuture<(File, BytesMut), io::Error>, usize),
    Ready(File, usize),
    Eof,
    Swapping,
}

impl FsReadStream {
    fn handle_read(
        &mut self,
        file: File,
        chunk: BytesMut,
        buf_size: usize,
    ) -> Poll<Option<<Self as Stream>::Item>, <Self as Stream>::Error> {
        if chunk.is_empty() {
            self.state = State::Eof;
            return Ok(Async::Ready(None));
        } else {
            self.buffer = chunk;
            self.state = State::Ready(file, buf_size);
            return Ok(Async::Ready(Some(self.buffer.take().freeze())));
        }
    }
}

impl Stream for FsReadStream {
    type Item = Bytes;
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        loop {
            match mem::replace(&mut self.state, State::Swapping) {
                State::Init(buf_size) => {
                    let path = self.path.clone();
                    self.state = State::Opening(
                        self.pool
                            .cpu_pool
                            .spawn_fn(move || open_and_read(&path, buf_size)),
                    );
                }
                State::Opening(mut cpu) => {
                    let polled = cpu.poll();
                    self.state = State::Opening(cpu);
                    let (file, chunk) = try_ready!(polled);
                    let buf_size = chunk.capacity();

                    return self.handle_read(file, chunk, buf_size);
                }
                State::Working(mut cpu, buf_size) => {
                    let polled = cpu.poll();
                    self.state = State::Working(cpu, buf_size);
                    let (file, chunk) = try_ready!(polled);

                    return self.handle_read(file, chunk, buf_size);
                }
                State::Ready(file, buf_size) => {
                    let buf = self.buffer.split_off(0);
                    self.state = State::Working(
                        self.pool
                            .cpu_pool
                            .spawn_fn(move || read(file, buf_size, buf)),
                        buf_size,
                    );
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

fn read(mut file: File, buf_size: usize, mut buf: BytesMut) -> io::Result<(File, BytesMut)> {
    if !buf.has_remaining_mut() {
        buf.reserve(buf_size);
    }
    let n = file.read(unsafe { buf.bytes_mut() })?;
    unsafe { buf.advance_mut(n) };
    Ok((file, buf))
}

fn open_and_read(path: &Path, buf_size: Option<usize>) -> io::Result<(File, BytesMut)> {
    let initial_cap = match fs::metadata(path) {
        Ok(metadata) => {
            // try to get the buffer size from the OS if necessary
            let buf_size = buf_size.unwrap_or_else(|| get_block_size(&metadata));

            // if size is smaller than our chunk size, don't reserve wasted space
            cmp::min(metadata.len() as usize, buf_size)
        }
        _ => buf_size.unwrap_or(BUF_SIZE),
    };

    let file = File::open(path)?;
    read(file, initial_cap, BytesMut::with_capacity(initial_cap))
}

#[cfg(unix)]
fn get_block_size(metadata: &Metadata) -> usize {
    use std::os::unix::fs::MetadataExt;
    metadata.blksize() as usize
}

#[cfg(not(unix))]
fn get_block_size(metadata: &Metadata) -> usize {
    BUF_SIZE
}
