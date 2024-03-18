pub mod convert;
pub mod las;
pub mod parquet;
pub mod ply;

pub const DEFAULT_BATCH_SIZE: usize = 1024 * 1024;

/// Point cloud reader trait
pub trait PointCloudReader<'a>: Sized {
    type T: arrow::record_batch::RecordBatchReader;

    fn from_path<P: AsRef<std::path::Path>>(path: P) -> std::io::Result<Self>;

    fn record_batch_reader(&'a mut self) -> Self::T;
}

/// Point cloud writer trait
pub trait PointCloudWriter: arrow::record_batch::RecordBatchWriter {}

/// Point cloud format extensions
#[derive(clap::ValueEnum, Debug, Clone)]
pub enum FormatExt {
    IPC,
    LAS,
    LAZ,
    Parquet,
    PLY,
}

impl AsRef<std::ffi::OsStr> for FormatExt {
    fn as_ref(&self) -> &std::ffi::OsStr {
        match self {
            FormatExt::IPC => std::ffi::OsStr::new("arrow"),
            FormatExt::LAS => std::ffi::OsStr::new("las"),
            FormatExt::LAZ => std::ffi::OsStr::new("laz"),
            FormatExt::Parquet => std::ffi::OsStr::new("parquet"),
            FormatExt::PLY => std::ffi::OsStr::new("ply"),
        }
    }
}

impl TryFrom<&std::ffi::OsStr> for FormatExt {
    type Error = String;

    fn try_from(value: &std::ffi::OsStr) -> Result<Self, Self::Error> {
        match value.to_ascii_lowercase().to_str() {
            Some("arrow") | Some("ipc") => Ok(FormatExt::IPC),
            Some("las") => Ok(FormatExt::LAS),
            Some("laz") => Ok(FormatExt::LAZ),
            Some("parquet") => Ok(FormatExt::Parquet),
            Some("ply") => Ok(FormatExt::PLY),
            x => Err(format!("Unknown extension `{x:?}`")),
        }
    }
}
