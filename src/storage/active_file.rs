use std::{
    io::{self, IoSlice},
    path::PathBuf,
    sync::Arc,
};

use tokio::{
    fs::File,
    io::{AsyncWriteExt, BufWriter},
};
use tracing::{debug, error};

use super::{constants::*, file_util::new_data_writer};
use crate::{storage::config::StorageConfig, utils::time::current_timestamp_ms};

pub struct WriteRecordResult {
    pub(crate) value_size: usize,
    pub(crate) value_pos: u64,
    pub(crate) file_id: u64,
    pub(crate) timestamp: u64,
}

pub struct ActiveFile {
    id: u64,
    next_id: u64,
    current_pos: u64,
    base_dir: PathBuf,
    config: Arc<dyn StorageConfig>, // 这里为什么要 dyn
    writer: BufWriter<File>,
    // 如果外层处理并发的方式不是 Actor 模型，而是单线程多任务，那么锁是必须的
    write_lock: tokio::sync::Mutex<()>,
}

impl ActiveFile {
    pub async fn new(
        base_dir: PathBuf,
        initial_id: u64,
        config: Arc<dyn StorageConfig>,
    ) -> io::Result<Self> {
        let writer = new_data_writer(&base_dir, initial_id, FILE_WRITER_BUFFER_SIZE).await?;
        let current_pos = writer.get_ref().metadata().await?.len();

        Ok(Self {
            writer,
            base_dir,
            config,
            current_pos,
            id: initial_id,
            next_id: initial_id + 1,
            write_lock: tokio::sync::Mutex::new(()),
        })
    }

    pub async fn write_record(
        &mut self,
        key: &[u8],
        value: &[u8],
    ) -> io::Result<WriteRecordResult> {
        let record_size = RECORD_HEADER_SIZE + key.len() + value.len();

        let timestamp = current_timestamp_ms();
        let mut header_bytes = [0u8; 16];
        let record = Self::record_to_io_slices(timestamp, key, value, &mut header_bytes);

        if self.should_rotate(record_size) {
            self.rotate().await?;
        }

        let _guard = self.write_lock.lock().await;
        let bytes_written = self.writer.write_vectored(&record).await?;

        // 下面两行会有数据竞争的并发问题吗，在这个异步的write_record函数内部？
        // - 会
        let start_pos = self.current_pos;
        self.current_pos += bytes_written as u64;

        Ok(WriteRecordResult {
            timestamp,
            file_id: self.id,
            value_size: value.len(),
            value_pos: start_pos + RECORD_HEADER_SIZE as u64 + key.len() as u64,
        })
    }

    pub fn id(&self) -> u64 {
        self.id
    }

    async fn rotate(&mut self) -> io::Result<()> {
        self.writer.flush().await?;

        let file_id = self.allocate_id();
        let new_writer = new_data_writer(&self.base_dir, file_id, FILE_WRITER_BUFFER_SIZE).await?;

        let old_writer = std::mem::replace(&mut self.writer, new_writer);
        let file_to_sync = old_writer.into_inner();
        let old_file_id = self.id;

        tokio::spawn(async move {
            if let Err(e) = Self::process_old_file(file_to_sync, old_file_id).await {
                error!("File {old_file_id} processing failed: {}", e);
            }
        });

        self.id = file_id;

        Ok(())
    }

    fn allocate_id(&mut self) -> u64 {
        let new_id = self.next_id;
        self.next_id += 1;
        new_id
    }

    #[inline]
    fn record_to_io_slices<'a>(
        timestamp: u64,
        key: &'a [u8],
        value: &'a [u8],
        header_bytes: &'a mut [u8; 16],
    ) -> [IoSlice<'a>; 5] {
        header_bytes[0..8].copy_from_slice(&timestamp.to_le_bytes());
        header_bytes[8..12].copy_from_slice(&(key.len() as u32).to_le_bytes());
        header_bytes[12..16].copy_from_slice(&(value.len() as u32).to_le_bytes());

        [
            IoSlice::new(&header_bytes[0..8]),
            IoSlice::new(&header_bytes[8..12]),
            IoSlice::new(&header_bytes[12..16]),
            IoSlice::new(key),
            IoSlice::new(value),
        ]
    }

    #[inline(always)]
    fn should_rotate(&self, new_record_size: usize) -> bool {
        self.current_pos + new_record_size as u64 >= self.config.max_active_file_size()
    }

    async fn process_old_file(file: File, id: u64) -> io::Result<()> {
        file.sync_all().await?;
        debug!("File {id} synced successfully");

        let mut perms = file.metadata().await?.permissions();
        perms.set_readonly(true);
        file.set_permissions(perms).await?;
        debug!("File {id} set to readonly successfully");

        // TODO: 是否要关闭 file 句柄

        Ok(())
    }
}
