use std::path::PathBuf;

pub const LOG_DIR: &str = "/tmp/kraft-combined-logs";

pub fn segment_path(topic: &str, partition: i32) -> PathBuf {
    PathBuf::from(format!(
        "{LOG_DIR}/{topic}-{partition}/00000000000000000000.log"
    ))
}

pub fn read_partition(topic: &str, partition: i32) -> Option<Vec<u8>> {
    std::fs::read(segment_path(topic, partition)).ok()
}
