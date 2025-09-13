use std::{
    io::{self, Error},
    num::NonZeroUsize,
    path::Path,
    sync::Arc,
};

use lru::LruCache;
use tokio::{
    fs::{File, OpenOptions},
    io::BufWriter,
};
use tracing::warn;

pub async fn new_data_writer(
    base_dir: &Path,
    file_id: u64,
    buffer_size: usize,
) -> io::Result<BufWriter<File>> {
    let path = base_dir.join(format!("{file_id:08}.data"));
    new_file_writer(&path, buffer_size).await
}

async fn new_file_writer(path: &Path, buffer_size: usize) -> io::Result<BufWriter<File>> {
    // let read_fd = OpenOptions::new().read(true).open(path).await?;
    // let mut reader = BufReader::with_capacity(constants::FILE_READER_BUFFER_SIZE, read_fd);
    // let mut buffer = [0u8, 10];
    // reader.read_exact(&mut buffer).await.unwrap();

    let write_fd = OpenOptions::new()
        .read(true)
        .create(true)
        .append(true)
        .open(path)
        .await?;

    let writer = BufWriter::with_capacity(buffer_size, write_fd);
    Ok(writer)
}

pub struct FileCache {
    inner: LruCache<u64, Arc<File>>,
}

impl FileCache {
    pub fn new(capacity: usize) -> Self {
        const MIN_CAPACITY: usize = 10;
        let capacity = if capacity == 0 {
            warn!("FileCache capacity cannot be zero, using default {MIN_CAPACITY}");
            MIN_CAPACITY
        } else {
            capacity
        };

        let capacity_nz =
            NonZeroUsize::new(capacity).expect("Capacity should be positive after adjustment");
        FileCache {
            inner: LruCache::new(capacity_nz),
        }
    }

    pub fn get(&mut self, file_id: u64) -> io::Result<Arc<File>> {
        match self.inner.get(&file_id) {
            Some(file) => Ok(Arc::clone(file)),
            _ => Err(Error::other(format!("file not in cache: {file_id}"))),
        }
    }

    pub fn insert(&mut self, file_id: u64, file: File) {
        self.inner.put(file_id, Arc::new(file));
    }
}
