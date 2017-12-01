extern crate futures;
extern crate futures_fs;

use std::{env, io};
use futures::{Future, Sink, Stream};
use futures_fs::FsPool;


#[test]
fn test_smoke() {
    let fs = FsPool::default();

    let mut tmp = env::temp_dir();
    tmp.push("futures-fs");

    let bytes = futures::stream::iter_ok::<_, io::Error>(
        vec!["hello", " ", "world"]
            .into_iter()
            .map(|piece| piece.into()),
    );

    bytes
        .forward(fs.write(tmp.clone(), Default::default()))
        .wait()
        .unwrap();

    let data = fs.read(tmp.clone(), Default::default())
        .collect()
        .wait()
        .unwrap()
        .concat();
    assert_eq!(data, b"hello world");
    fs.delete(tmp).wait().unwrap();
}


#[test]
fn test_smoke_long() {
    let fs = FsPool::default();

    let mut tmp = env::temp_dir();
    tmp.push("futures-fs-long");

    let mut sink = fs.write(tmp.clone(), Default::default());
    for i in 0..10 {
        sink = sink.send(vec![i + 1; 4096].into()).wait().unwrap();
    }

    let mut data = Vec::new();
    for chunk in fs.read(tmp.clone(), Default::default()).wait() {
        data.extend_from_slice(chunk.unwrap().as_ref());
    }

    assert_eq!(data.len(), 4096 * 10);
    assert_eq!(&data[..4096], &[1u8; 4096][..]);
    assert_eq!(&data[4096..8192], &[2u8; 4096][..]);

    fs.delete(tmp).wait().unwrap();
}
