// ============================================================
// standard_file_system.rs — Local filesystem implementation
//
// Implements the FileHandle / FileSystem traits using std::fs.
// ============================================================

use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};

use crate::common::errors::StorageResult;

use super::storage_info::{FileHandle, FileOpenFlags, FileSystem, StorageError};

// ── LocalFileHandle ───────────────────────────────────────────

pub struct LocalFileHandle {
    path: String,
    file: File,
}

impl FileHandle for LocalFileHandle {
    fn read_at(&mut self, buf: &mut [u8], offset: u64) -> StorageResult<()> {
        self.file.seek(SeekFrom::Start(offset))?;
        self.file.read_exact(buf)?;
        Ok(())
    }

    fn write_at(&mut self, buf: &[u8], offset: u64) -> StorageResult<()> {
        self.file.seek(SeekFrom::Start(offset))?;
        self.file.write_all(buf)?;
        Ok(())
    }

    fn truncate(&mut self, new_size: u64) -> StorageResult<()> {
        self.file.set_len(new_size)?;
        Ok(())
    }

    fn sync(&mut self) -> StorageResult<()> {
        self.file.sync_all()?;
        Ok(())
    }

    fn path(&self) -> &str {
        &self.path
    }

    fn file_size(&self) -> StorageResult<u64> {
        let meta = self.file.metadata()?;
        Ok(meta.len())
    }
}

// ── LocalFileSystem ───────────────────────────────────────────

pub struct LocalFileSystem;

impl FileSystem for LocalFileSystem {
    fn open_file(&self, path: &str, flags: FileOpenFlags) -> StorageResult<Box<dyn FileHandle>> {
        let read = flags.contains(FileOpenFlags::READ);
        let write = flags.contains(FileOpenFlags::WRITE);
        let create = flags.contains(FileOpenFlags::CREATE);
        let truncate = flags.contains(FileOpenFlags::TRUNCATE);

        let file = OpenOptions::new()
            .read(read)
            .write(write || create)
            .create(create)
            .truncate(truncate)
            .open(path)
            .map_err(|e| {
                if e.kind() == std::io::ErrorKind::NotFound {
                    StorageError::NotFound {
                        path: path.to_string(),
                    }
                } else {
                    StorageError::Io(e)
                }
            })?;

        Ok(Box::new(LocalFileHandle {
            path: path.to_string(),
            file,
        }))
    }

    fn file_exists(&self, path: &str) -> bool {
        std::path::Path::new(path).exists()
    }

    fn try_remove_file(&self, path: &str) {
        let _ = fs::remove_file(path);
    }

    fn remove_file(&self, path: &str) -> StorageResult<()> {
        fs::remove_file(path)?;
        Ok(())
    }

    fn move_file(&self, from: &str, to: &str) -> StorageResult<()> {
        fs::rename(from, to)?;
        Ok(())
    }

    fn create_directory(&self, path: &str) -> StorageResult<()> {
        fs::create_dir_all(path)?;
        Ok(())
    }

    fn list_files(&self, path: &str) -> StorageResult<Vec<String>> {
        let entries = fs::read_dir(path)?;
        let mut result = Vec::new();
        for entry in entries {
            let entry = entry?;
            if let Some(name) = entry.file_name().to_str() {
                result.push(name.to_string());
            }
        }
        Ok(result)
    }

    fn join_path(&self, base: &str, name: &str) -> String {
        if base.is_empty() {
            return name.to_string();
        }
        let sep = if base.ends_with('/') || base.ends_with('\\') {
            ""
        } else {
            "/"
        };
        format!("{base}{sep}{name}")
    }
}
