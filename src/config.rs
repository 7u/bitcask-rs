use crate::storage::config::StorageConfig;
#[derive(Debug, Clone)]
pub struct BitCaskConfig {
    pub max_active_file_size: u64,
}

impl Default for BitCaskConfig {
    fn default() -> Self {
        Self {
            max_active_file_size: 64 * 1024 * 1024,
        }
    }
}

impl StorageConfig for BitCaskConfig {
    fn max_active_file_size(&self) -> u64 {
        self.max_active_file_size
    }
}
