use std::{
    collections::HashMap,
    io,
    path::{Path, PathBuf},
    pin::Pin,
    sync::Arc,
};

use tokio::{
    fs::{self, File, OpenOptions},
    io::{AsyncReadExt, AsyncSeekExt, BufReader},
    sync::{Semaphore, mpsc as tk_mpsc},
    task::JoinSet,
};

use super::{
    WriteRecordResult, active_file::ActiveFile, config::StorageConfig, constants::*,
    file_util::FileCache,
};

struct Entry {
    value_size: usize,
    value_pos: u64,
    file_id: u64,
    timestamp: u64,
}

impl Entry {
    fn new(file_id: u64, value_pos: u64, value_size: usize, timestamp: u64) -> Self {
        Entry {
            file_id,
            value_pos,
            value_size,
            timestamp,
        }
    }
}

#[derive(PartialEq)]
enum FileType {
    Hint,
    Data,
}

struct FileInfo {
    file_type: FileType,
    id: u64,
    path: PathBuf,
}

struct DataDirScanResult {
    hint_files: Vec<FileInfo>,
    data_files: Vec<FileInfo>,
}

pub struct BitCaskHandle<C: StorageConfig> {
    base_dir: PathBuf,
    config: Arc<C>,
    read_files: FileCache,
    active_file: ActiveFile,
    hint_files: Option<Vec<File>>,
    keydir: HashMap<Vec<u8>, Entry>,
}

impl<C> BitCaskHandle<C>
where
    C: StorageConfig + Default,
{
    pub async fn open(dir: impl Into<PathBuf>) -> io::Result<Self> {
        Self::open_with_config(dir, C::default()).await
    }
}

impl<C: StorageConfig> BitCaskHandle<C> {
    pub async fn open_with_config(dir: impl Into<PathBuf>, config: C) -> io::Result<Self> {
        let base_dir = dir.into();
        let scan_result = Self::scan_data_dir(&base_dir).await?;

        let mut max_id = scan_result
            .data_files
            .iter()
            .map(|f| f.id)
            .max()
            .unwrap_or(0u64);
        max_id = max_id.max(
            scan_result
                .hint_files
                .iter()
                .map(|f| f.id)
                .max()
                .unwrap_or(0u64),
        );
        let initial_id = if max_id == 0 { 0 } else { max_id + 1 };

        let config = Arc::new(config);

        let active_file =
            ActiveFile::new(PathBuf::from(&base_dir), initial_id, config.clone()).await?;

        let keydir = Self::build_keydir(scan_result).await?;

        Ok(BitCaskHandle {
            keydir,
            base_dir,
            config,
            active_file,
            hint_files: None,
            read_files: FileCache::new(READ_FILES_CACHE_SIZE),
        })
    }

    pub async fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        None
    }

    pub async fn put(&mut self, key: &[u8], value: &[u8]) -> io::Result<()> {
        // TODO: 写文件 和 更新 keydir 需要是一整个原子操作
        // 如果 多线程多任务，RwLock，那么写锁要锁整个handle吗？
        // 这样，put内部调用栈上的各处await又有何意义？并发体现在何处？
        // 同理，如果是（单线程多任务）单线程异步，这些await点的意义又何在？是多个put的IO可以交替并发？
        let WriteRecordResult {
            file_id,
            value_pos,
            value_size,
            timestamp,
        } = self.active_file.write_record(key, value).await?;

        self.update_keydir(key, file_id, value_pos, value_size, timestamp);

        Ok(())
    }

    pub fn active_file_id(&self) -> u64 {
        self.active_file.id()
    }

    async fn scan_data_dir(base_dir: &Path) -> io::Result<DataDirScanResult> {
        #[allow(unused_doc_comments)]
        /// for file in base_dir:
        ///     file_type  = get_type(file)
        ///     max_id = 0
        ///     build file info map:
        ///         put file_id in file_map if not exists:
        ///             <file_id, (file_handle, [file_type_list])>
        ///             max_id = max(max_id, file_id)
        ///         or
        ///         put file_id in hint_file_map if not exists:
        ///             <file_id, file_handle>
        ///             max_id = max(max_id, file_id)
        ///         put file_id in data_file_map if not exists:
        ///             <file_id, file_handle>
        ///             max_id = max(max_id, file_id)
        ///         data_file_map.zip(hint_file_map).filter(fild_id in data_file_map && not in hint_file_map)
        ///
        ///     
        /// for (file_id, file_handle) in hint_file_map:
        ///     async_process_hint_file(file_handle, keydir)
        ///     
        /// for (file_id, file_handle) in data_file_map:
        ///     async_process_data_file(file_handle, keydir)
        ///
        /// async_process_file(file_handle, keydir):
        ///     for content in batch_read(file_handle):
        ///         (key, timestamp) = decode(content)
        ///         if timestamp > keydir(key).timestamp:
        ///             update keydir
        ///     
        //
        let mut hint_file_map = HashMap::new();
        let mut data_file_map = HashMap::new();

        let mut entries = fs::read_dir(base_dir).await?;
        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            if !path.is_file() {
                continue;
            }

            let (Some(file_name), Some(ext)) = (
                path.file_name().and_then(|x| x.to_str()),
                path.extension().and_then(|x| x.to_str()),
            ) else {
                continue;
            };

            if let Ok(id) = file_name.parse::<u64>() {
                match ext {
                    "log" => {
                        data_file_map.entry(id).or_insert(path.clone());
                    }
                    "hint" => {
                        hint_file_map.entry(id).or_insert(path.clone());
                    }
                    _ => continue,
                }
            }
        }

        data_file_map.retain(|file_id, _| !hint_file_map.contains_key(file_id));

        let hint_file_list: Vec<FileInfo> = hint_file_map
            .into_iter()
            .map(|(file_id, path)| FileInfo {
                id: file_id,
                file_type: FileType::Hint,
                path,
            })
            .collect();

        let data_file_list: Vec<FileInfo> = data_file_map
            .into_iter()
            .map(|(file_id, path)| FileInfo {
                id: file_id,
                file_type: FileType::Data,
                path,
            })
            .collect();

        Ok(DataDirScanResult {
            hint_files: hint_file_list,
            data_files: data_file_list,
        })
    }

    async fn build_keydir(scan_res: DataDirScanResult) -> io::Result<HashMap<Vec<u8>, Entry>> {
        let (tx, mut rx) = tk_mpsc::channel(100);
        let semaphore = Arc::new(Semaphore::new(num_cpus::get() * 2));
        let mut tasks = JoinSet::<io::Result<()>>::new();

        type FileProcessorFn = fn(
            PathBuf,
            u64,
        ) -> Pin<
            Box<dyn Future<Output = io::Result<HashMap<Vec<u8>, Entry>>> + Send>,
        >;

        let spawn_file_task = |tasks: &mut JoinSet<io::Result<()>>,
                               tx: &tk_mpsc::Sender<HashMap<Vec<u8>, Entry>>,
                               semaphore: &Arc<Semaphore>,
                               file: FileInfo,
                               processor: FileProcessorFn| {
            let tx = tx.clone();
            let semaphore = semaphore.clone();

            tasks.spawn(async move {
                let _permit = semaphore.acquire().await.map_err(io::Error::other)?;
                // let _permit = semaphore.acquire_owned().await.map_err(io::Error::other)?;

                let entries = processor(file.path, file.id).await?;

                tx.send(entries)
                    .await
                    .map_err(|e| io::Error::new(io::ErrorKind::BrokenPipe, e))?;

                Ok(())
            });
        };

        let mut keydir: HashMap<Vec<u8>, Entry> = HashMap::new();

        for hint_file in scan_res.hint_files {
            spawn_file_task(&mut tasks, &tx, &semaphore, hint_file, |path, id| {
                Box::pin(Self::process_hint_file(path, id))
            });
        }
        drop(tx);

        while let Some(entries) = rx.recv().await {
            keydir.extend(entries);
        }

        let (tx, mut rx) = tk_mpsc::channel(100);

        for data_file in scan_res.data_files {
            spawn_file_task(&mut tasks, &tx, &semaphore, data_file, |path, id| {
                Box::pin(Self::process_data_file(path, id))
            });
        }
        drop(tx);

        while let Some(entries) = rx.recv().await {
            for (key, new_entry) in entries {
                if let Some(entry) = keydir.get(&key) {
                    if new_entry.timestamp <= entry.timestamp {
                        continue;
                    }
                    keydir.insert(key, new_entry);
                }
            }
        }

        // while tasks.join_next().await.is_some() {}
        while let Some(res) = tasks.join_next().await {
            res??;
        }

        Ok(keydir)
    }

    async fn process_data_file(path: PathBuf, file_id: u64) -> io::Result<HashMap<Vec<u8>, Entry>> {
        let mut res_map: HashMap<Vec<u8>, Entry> = HashMap::new();
        let file = OpenOptions::new().read(true).open(&path).await?;
        let mut reader = BufReader::with_capacity(FILE_READER_BUFFER_SIZE, file);

        // TODO cap as follow:
        // while let Some(entry) = read_data_record(&mut reader).await? {
        // }
        let mut offset = 0u64;
        let mut header_bytes = [0u8; RECORD_HEADER_SIZE];
        while (reader.read_exact(&mut header_bytes).await).is_ok() {
            let tstamp_bytes = <[u8; 8]>::try_from(&header_bytes[0..8]).unwrap();
            let timestamp = u64::from_be_bytes(tstamp_bytes);

            let key_size_bytes = <[u8; 4]>::try_from(&header_bytes[8..12]).unwrap();
            let key_size = u32::from_be_bytes(key_size_bytes);
            let mut key = vec![0u8; key_size as usize];

            if reader.read_exact(&mut key).await.is_err() {
                break;
            }

            let entry = res_map.entry(key).or_insert_with(|| Entry::new(0, 0, 0, 0));
            if timestamp <= entry.timestamp {
                continue;
            }

            let value_pos = offset + RECORD_HEADER_SIZE as u64 + key_size as u64;
            let value_size_bytes = <[u8; 4]>::try_from(&header_bytes[12..16]).unwrap();
            let value_size = u32::from_be_bytes(value_size_bytes);

            entry.file_id = file_id;
            entry.value_pos = value_pos;
            entry.value_size = value_size as usize;
            entry.timestamp = timestamp;

            // seek to end of value, update offset
            reader
                .seek(io::SeekFrom::Current(value_size as i64))
                .await?;
            offset += value_pos + value_size as u64;
        }
        Ok(res_map)
    }

    async fn process_hint_file(path: PathBuf, file_id: u64) -> io::Result<HashMap<Vec<u8>, Entry>> {
        let mut res_map: HashMap<Vec<u8>, Entry> = HashMap::new();
        let file = OpenOptions::new().read(true).open(&path).await?;
        let mut reader = BufReader::with_capacity(FILE_READER_BUFFER_SIZE, file);

        let mut header_bytes = [0u8; HINT_HEADER_SIZE];
        while (reader.read_exact(&mut header_bytes).await).is_ok() {
            let tstamp_bytes = <[u8; 8]>::try_from(&header_bytes[0..8]).unwrap();
            let timestamp = u64::from_be_bytes(tstamp_bytes);

            let key_size_bytes = <[u8; 4]>::try_from(&header_bytes[8..12]).unwrap();
            let key_size = u32::from_be_bytes(key_size_bytes);

            let value_size_bytes = <[u8; 4]>::try_from(&header_bytes[12..16]).unwrap();
            let value_size = u32::from_be_bytes(value_size_bytes);

            let value_pos_bytes = <[u8; 8]>::try_from(&header_bytes[16..24]).unwrap();
            let value_pos = u64::from_be_bytes(value_pos_bytes);

            let mut key = vec![0u8; key_size as usize];
            if reader.read_exact(&mut key).await.is_err() {
                break;
            }

            res_map.insert(
                key,
                Entry {
                    timestamp,
                    value_size: value_size as usize,
                    value_pos,
                    file_id,
                },
            );
        }

        Ok(res_map)
    }

    fn update_keydir(
        &mut self,
        key: &[u8],
        file_id: u64,
        value_pos: u64,
        value_size: usize,
        timestamp: u64,
    ) {
        let entry = self
            .keydir
            .entry(key.to_vec())
            .or_insert_with(|| Entry::new(0, 0, 0, 0));

        // 比较 timestamp
        if entry.timestamp < timestamp {
            entry.file_id = file_id;
            entry.value_pos = value_pos;
            entry.value_size = value_size;
            entry.timestamp = timestamp;
        }
    }
}
