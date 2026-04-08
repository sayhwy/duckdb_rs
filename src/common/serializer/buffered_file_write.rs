#![allow(non_snake_case)]

use crate::common::errors::StorageResult;
use crate::common::serializer::write_stream::WriteStream;
use crate::storage::storage_info::{FileHandle, FileOpenFlags, FileSystem, StorageError};

pub const FILE_BUFFER_SIZE: usize = 4096;

pub struct BufferedFileWriter<'fs> {
    pub fs: &'fs dyn FileSystem,
    pub path: String,
    pub data: Vec<u8>,
    pub offset: usize,
    pub total_written: u64,
    pub handle: Option<Box<dyn FileHandle>>,
}

impl<'fs> BufferedFileWriter<'fs> {
    pub const DEFAULT_OPEN_FLAGS: FileOpenFlags =
        FileOpenFlags(FileOpenFlags::WRITE.0 | FileOpenFlags::CREATE.0);

    // Serializes to a buffer allocated by the serializer, will expand when
    // writing past the initial threshold.
    pub fn new(
        fs: &'fs dyn FileSystem,
        path: impl Into<String>,
        open_flags: FileOpenFlags,
    ) -> StorageResult<Self> {
        let path = path.into();
        let handle = fs.open_file(&path, open_flags)?;
        Ok(Self {
            fs,
            path,
            data: vec![0; FILE_BUFFER_SIZE],
            offset: 0,
            total_written: 0,
            handle: Some(handle),
        })
    }

    pub fn with_default_flags(
        fs: &'fs dyn FileSystem,
        path: impl Into<String>,
    ) -> StorageResult<Self> {
        Self::new(fs, path, Self::DEFAULT_OPEN_FLAGS)
    }

    pub fn GetFileSize(&self) -> StorageResult<u64> {
        let physical_size = self
            .handle
            .as_ref()
            .ok_or_else(|| StorageError::Other("BufferedFileWriter is closed".into()))?
            .file_size()?;
        Ok(physical_size + self.offset as u64)
    }

    pub fn GetTotalWritten(&self) -> u64 {
        self.total_written + self.offset as u64
    }

    pub fn Close(&mut self) -> StorageResult<()> {
        self.Flush()?;
        self.handle.take();
        Ok(())
    }

    pub fn Sync(&mut self) -> StorageResult<()> {
        self.Flush()?;
        self.handle_mut()?.sync()
    }

    pub fn Flush(&mut self) -> StorageResult<()> {
        if self.offset == 0 {
            return Ok(());
        }
        let write_offset = self.total_written;
        let pending = self.offset;
        let flush_data = self.data[..pending].to_vec();
        self.handle_mut()?.write_at(&flush_data, write_offset)?;
        self.total_written += pending as u64;
        self.offset = 0;
        Ok(())
    }

    pub fn Truncate(&mut self, size: u64) -> StorageResult<()> {
        let persistent = self
            .handle
            .as_ref()
            .ok_or_else(|| StorageError::Other("BufferedFileWriter is closed".into()))?
            .file_size()?;
        debug_assert!(size <= persistent + self.offset as u64);
        if size > persistent + self.offset as u64 {
            return Err(StorageError::Other(format!(
                "truncate size {} exceeds buffered size {}",
                size,
                persistent + self.offset as u64
            )));
        }
        if persistent <= size {
            self.offset = (size - persistent) as usize;
        } else {
            self.handle_mut()?.truncate(size)?;
            self.offset = 0;
            self.total_written = size;
        }
        Ok(())
    }

    pub fn get_file_size(&self) -> StorageResult<u64> {
        self.GetFileSize()
    }

    pub fn get_total_written(&self) -> u64 {
        self.GetTotalWritten()
    }

    pub fn close(&mut self) -> StorageResult<()> {
        self.Close()
    }

    pub fn sync(&mut self) -> StorageResult<()> {
        self.Sync()
    }

    pub fn flush(&mut self) -> StorageResult<()> {
        self.Flush()
    }

    pub fn truncate(&mut self, size: u64) -> StorageResult<()> {
        self.Truncate(size)
    }

    fn handle_mut(&mut self) -> StorageResult<&mut (dyn FileHandle + 'static)> {
        self.handle
            .as_deref_mut()
            .ok_or_else(|| StorageError::Other("BufferedFileWriter is closed".into()))
    }
}

impl<'fs> WriteStream for BufferedFileWriter<'fs> {
    fn WriteData(&mut self, buffer: &[u8], write_size: usize) -> StorageResult<()> {
        if write_size > buffer.len() {
            return Err(StorageError::Other(format!(
                "requested write_size {} exceeds buffer length {}",
                write_size,
                buffer.len()
            )));
        }

        let buffer = &buffer[..write_size];
        if write_size >= (2 * FILE_BUFFER_SIZE - self.offset) {
            let mut to_copy = 0usize;
            if self.offset != 0 {
                to_copy = FILE_BUFFER_SIZE - self.offset;
                self.data[self.offset..self.offset + to_copy].copy_from_slice(&buffer[..to_copy]);
                self.offset += to_copy;
                self.Flush()?;
            }

            let remaining_to_write = write_size - to_copy;
            if remaining_to_write > 0 {
                let write_offset = self.total_written;
                self.handle_mut()?
                    .write_at(&buffer[to_copy..to_copy + remaining_to_write], write_offset)?;
                self.total_written += remaining_to_write as u64;
            }
            Ok(())
        } else {
            let mut buffer_offset = 0usize;
            while buffer_offset < write_size {
                let to_write = (write_size - buffer_offset).min(FILE_BUFFER_SIZE - self.offset);
                debug_assert!(to_write > 0);
                self.data[self.offset..self.offset + to_write]
                    .copy_from_slice(&buffer[buffer_offset..buffer_offset + to_write]);
                self.offset += to_write;
                buffer_offset += to_write;
                if self.offset == FILE_BUFFER_SIZE {
                    self.Flush()?;
                }
            }
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use crate::storage::standard_file_system::LocalFileSystem;

    use super::*;

    fn temp_path(name: &str) -> String {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir()
            .join(format!("duckdb_rs_{name}_{nanos}.bin"))
            .to_string_lossy()
            .into_owned()
    }

    #[test]
    fn buffered_file_writer_roundtrip_state() {
        let fs = LocalFileSystem;
        let path = temp_path("writer_roundtrip");
        let mut writer = BufferedFileWriter::with_default_flags(&fs, path.clone()).unwrap();

        writer.WriteData(b"hello", 5).unwrap();
        assert_eq!(writer.GetTotalWritten(), 5);
        assert_eq!(writer.GetFileSize().unwrap(), 5);

        writer.Flush().unwrap();
        assert_eq!(writer.GetTotalWritten(), 5);
        assert_eq!(writer.handle.as_ref().unwrap().file_size().unwrap(), 5);

        fs.try_remove_file(&path);
    }

    #[test]
    fn buffered_file_writer_direct_write_branch() {
        let fs = LocalFileSystem;
        let path = temp_path("writer_direct");
        let mut writer = BufferedFileWriter::with_default_flags(&fs, path.clone()).unwrap();

        writer.WriteData(b"abc", 3).unwrap();
        let large = vec![7u8; FILE_BUFFER_SIZE * 2];
        writer.WriteData(&large, large.len()).unwrap();
        writer.Close().unwrap();

        let mut handle = fs.open_file(&path, FileOpenFlags::READ).unwrap();
        let mut data = vec![0u8; 3 + FILE_BUFFER_SIZE * 2];
        handle.read_at(&mut data, 0).unwrap();
        assert_eq!(&data[..3], b"abc");
        assert_eq!(&data[3..], &large);

        fs.try_remove_file(&path);
    }
}
