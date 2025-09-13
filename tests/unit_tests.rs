use std::{
    fs::OpenOptions,
    io::{BufWriter, Write},
    path::Path,
    u64,
};

use bitcask::{BitCaskConfig, BitCaskHandle, StorageConfig};
use ctor::ctor;
use tempfile::tempdir;
use tokio::time::{Duration, sleep};
use tracing_subscriber::EnvFilter;

#[derive(Clone)]
struct TestConfig {
    max_file_size: u64,
}

impl StorageConfig for TestConfig {
    fn max_active_file_size(&self) -> u64 {
        self.max_file_size
    }
}

#[ctor]
fn init() {
    if cfg!(test) {
        let filter = if std::env::var("RUST_LOG").is_err() {
            EnvFilter::new("warn,bitcask=info")
        } else {
            EnvFilter::from_default_env()
        };

        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_test_writer()
            .init();
    }
}

#[tokio::test]
async fn test_open() {
    let base_dir = tempdir().unwrap();

    let handle = BitCaskHandle::<BitCaskConfig>::open(base_dir.path()).await;
    assert!(handle.is_ok());
}

#[tokio::test]
async fn test_active_file_rotation() {
    let base_dir = tempdir().unwrap();

    let config = TestConfig { max_file_size: 100 };
    let mut handle = BitCaskHandle::<TestConfig>::open_with_config(base_dir.path(), config)
        .await
        .unwrap();

    for i in 0..20 {
        let key = format!("key_{i}").into_bytes();
        let value = vec![b'x', 50];
        handle.put(&key, &value).await.unwrap();
    }

    // 给异步任务足够的时间去完成
    sleep(Duration::from_millis(100)).await;

    // 循环多次, 显式推进任务队列，确保 async 任务都已经完成
    for _ in 0..20 {
        tokio::task::yield_now().await;
    }

    // let files = std::fs::read_dir(base_dir.path()).unwrap();
    // let file_metas: Vec<_> = files
    //     .filter_map(|e| e.ok())
    //     .filter(|e| e.path().extension().map(|e| e == "data").unwrap_or(false))
    //     .map(|e| (e.path(), e.metadata().unwrap()))
    //     .collect();

    let mut file_metas = vec![];
    let mut files = tokio::fs::read_dir(base_dir.path()).await.unwrap();
    while let Some(entry) = files.next_entry().await.unwrap() {
        let path = entry.path();
        if path.extension().map(|e| e == "data").unwrap_or(false) {
            let meta = entry.metadata().await.unwrap();
            file_metas.push((path, meta));
        }
    }
    //

    assert!(file_metas.len() >= 2, "File rotation did not occur");

    let active_file_id = handle.active_file_id();
    for (path, meta) in file_metas {
        let file_id = path
            .file_stem()
            .and_then(|s| s.to_str())
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(u64::MAX);

        let should_be_readonly = file_id != active_file_id;

        assert_eq!(
            meta.permissions().readonly(),
            should_be_readonly,
            "File {:?} should {} be readonly (active ID: {active_file_id})",
            path,
            if should_be_readonly { "" } else { "not" }
        );
    }
}

fn create_mock_hint_file(dir: &Path, file_id: u64, entries: &[(u32, u64, &[u8])]) {
    let path = dir.join(format!("{file_id:08}"));
    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .unwrap();
    let mut writer = BufWriter::new(file);
    for (value_size, value_pos, key) in entries {
        writer.write_all(&0u64.to_le_bytes()).unwrap();
        writer.write_all(&(key.len() as u32).to_le_bytes()).unwrap();
        writer.write_all(&value_size.to_le_bytes()).unwrap();
        writer.write_all(&value_pos.to_le_bytes()).unwrap();
        writer.write_all(key).unwrap();
    }
    writer.flush().unwrap();
}
