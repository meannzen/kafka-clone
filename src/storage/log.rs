use std::io::Write;
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

pub fn write_log(topic: &str, partition: i32, records: &[u8]) -> crate::Result<()> {
    let path = segment_path(topic, partition);
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir)?;
    }
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)?;
    file.write_all(records)?;
    file.sync_data()?;
    Ok(())
}
