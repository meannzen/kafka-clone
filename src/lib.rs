pub mod connection;
pub mod metadata;
pub mod protocol;
pub mod service;

pub type Error = Box<dyn std::error::Error + Send + Sync>;
pub type Result<T> = std::result::Result<T, Error>;

pub const PORT: u16 = 9092;
