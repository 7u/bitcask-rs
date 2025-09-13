pub trait StorageConfig: Send + Sync + 'static {
    fn max_active_file_size(&self) -> u64;
}
