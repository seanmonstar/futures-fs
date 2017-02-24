extern crate futures;
extern crate futures_fs;

use std::env;
use futures::{Future, Sink, Stream};
use futures_fs::FsPool;


#[test]
fn test_smoke() {
    let fs = FsPool::default();

    let mut tmp = env::temp_dir();
    tmp.push("futures-fs");

    let bytes = futures::stream::iter(vec!["hello", " ", "world"].into_iter().map(|piece| {
        Ok::<_, ::std::io::Error>(piece)
    }));

    bytes.forward(fs.write(tmp.clone())).wait().unwrap();

    let data = fs.read(tmp.clone()).collect().wait().unwrap().concat();
    assert_eq!(data, b"hello world");
    fs.delete(tmp).wait().unwrap();
}
