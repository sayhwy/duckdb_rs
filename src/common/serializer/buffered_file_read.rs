#![allow(non_snake_case)]

use crate::common::errors::StorageResult;
use crate::common::serializer::read_stream::{QueryContext, ReadStream};
use crate::storage::storage_info::{FileHandle, FileOpenFlags, FileSystem, StorageError};

use super::buffered_file_write::FILE_BUFFER_SIZE;

#[allow(non_camel_case_types)]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum FileLockType {
    #[default]
    READ_LOCK,
    WRITE_LOCK,
}

#[derive(Debug, Default)]
pub struct FileOpener;

pub struct BufferedFileReader<'fs> {
    pub fs: &'fs dyn FileSystem,
    pub data: Vec<u8>,
    pub offset: usize,
    pub read_data: usize,
    pub handle: Box<dyn FileHandle>,
    file_size: u64,
    total_read: u64,
}

impl<'fs> BufferedFileReader<'fs> {
    pub fn new(
        fs: &'fs dyn FileSystem,
        path: &str,
        _lock_type: FileLockType,
        _opener: Option<&FileOpener>,
    ) -> StorageResult<Self> {
        let handle = fs.open_file(path, FileOpenFlags::READ)?;
        Self::from_handle(fs, handle)
    }

    pub fn from_handle(
        fs: &'fs dyn FileSystem,
        handle: Box<dyn FileHandle>,
    ) -> StorageResult<Self> {
        let file_size = handle.file_size()?;
        Ok(Self {
            fs,
            data: vec![0; FILE_BUFFER_SIZE],
            offset: 0,
            read_data: 0,
            handle,
            file_size,
            total_read: 0,
        })
    }

    pub fn Finished(&self) -> bool {
        self.total_read + self.offset as u64 == self.file_size
    }

    pub fn FileSize(&self) -> u64 {
        self.file_size
    }

    pub fn Reset(&mut self) {
        self.total_read = 0;
        self.read_data = 0;
        self.offset = 0;
    }

    pub fn Seek(&mut self, location: u64) -> StorageResult<()> {
        debug_assert!(location <= self.file_size);
        if location > self.file_size {
            return Err(StorageError::Other(format!(
                "seek location {} exceeds file size {}",
                location, self.file_size
            )));
        }
        self.total_read = location;
        self.read_data = 0;
        self.offset = 0;
        Ok(())
    }

    pub fn CurrentOffset(&self) -> u64 {
        self.total_read + self.offset as u64
    }

    pub fn finished(&self) -> bool {
        self.Finished()
    }

    pub fn file_size(&self) -> u64 {
        self.FileSize()
    }

    pub fn reset(&mut self) {
        self.Reset()
    }

    pub fn seek(&mut self, location: u64) -> StorageResult<()> {
        self.Seek(location)
    }

    pub fn current_offset(&self) -> u64 {
        self.CurrentOffset()
    }
}

impl<'fs> ReadStream for BufferedFileReader<'fs> {
    fn ReadData(&mut self, buffer: &mut [u8], read_size: usize) -> StorageResult<()> {
        self.ReadDataWithContext(QueryContext::default(), buffer, read_size)
    }

    fn ReadDataWithContext(
        &mut self,
        _context: QueryContext,
        buffer: &mut [u8],
        read_size: usize,
    ) -> StorageResult<()> {
        if read_size > buffer.len() {
            return Err(StorageError::Other(format!(
                "requested read_size {} exceeds buffer length {}",
                read_size,
                buffer.len()
            )));
        }

        let mut target_buffer = &mut buffer[..read_size];
        while !target_buffer.is_empty() {
            let available = self.read_data.saturating_sub(self.offset);
            let to_read = target_buffer.len().min(available);
            if to_read > 0 {
                let source = &self.data[self.offset..self.offset + to_read];
                let (target_head, target_tail) = target_buffer.split_at_mut(to_read);
                target_head.copy_from_slice(source);
                self.offset += to_read;
                target_buffer = target_tail;
            }

            if !target_buffer.is_empty() {
                debug_assert!(self.offset == self.read_data);
                self.total_read += self.read_data as u64;
                self.offset = 0;

                let remaining_in_file = self.file_size.saturating_sub(self.total_read) as usize;
                self.read_data = remaining_in_file.min(FILE_BUFFER_SIZE);
                if self.read_data == 0 {
                    return Err(StorageError::Corrupt {
                        msg: "not enough data in file to deserialize result".into(),
                    });
                }
                self.handle
                    .read_at(&mut self.data[..self.read_data], self.total_read)?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use crate::common::serializer::write_stream::WriteStream;
    use crate::storage::standard_file_system::LocalFileSystem;

    use super::*;
    use crate::common::serializer::buffered_file_write::BufferedFileWriter;

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
    fn buffered_file_reader_roundtrip() {
        let fs = LocalFileSystem;
        let path = temp_path("reader_roundtrip");
        let payload = vec![5u8; FILE_BUFFER_SIZE + 13];

        let mut writer = BufferedFileWriter::with_default_flags(&fs, path.clone()).unwrap();
        writer.WriteData(&payload, payload.len()).unwrap();
        writer.Close().unwrap();

        let mut reader =
            BufferedFileReader::new(&fs, &path, FileLockType::READ_LOCK, None).unwrap();
        let mut actual = vec![0u8; payload.len()];
        let actual_len = actual.len();
        reader.ReadData(&mut actual, actual_len).unwrap();

        assert_eq!(actual, payload);
        assert!(reader.Finished());

        fs.try_remove_file(&path);
    }

    #[test]
    fn buffered_file_reader_seek_and_reset() {
        let fs = LocalFileSystem;
        let path = temp_path("reader_seek");

        let mut writer = BufferedFileWriter::with_default_flags(&fs, path.clone()).unwrap();
        writer.WriteData(b"abcdef", 6).unwrap();
        writer.Close().unwrap();

        let mut reader =
            BufferedFileReader::new(&fs, &path, FileLockType::READ_LOCK, None).unwrap();
        reader.Seek(2).unwrap();
        let mut tail = [0u8; 2];
        reader.ReadData(&mut tail, 2).unwrap();
        assert_eq!(&tail, b"cd");
        assert_eq!(reader.CurrentOffset(), 4);

        reader.Reset();
        let mut head = [0u8; 3];
        reader.ReadData(&mut head, 3).unwrap();
        assert_eq!(&head, b"abc");

        fs.try_remove_file(&path);
    }
}
