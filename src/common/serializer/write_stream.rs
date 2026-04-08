#![allow(non_snake_case)]

use std::mem::size_of;

use crate::common::errors::StorageResult;

pub trait WriteStream {
    // Writes a set amount of data from the specified buffer into the stream and
    // moves the stream forward accordingly.
    fn WriteData(&mut self, buffer: &[u8], write_size: usize) -> StorageResult<()>;

    fn write_data(&mut self, buffer: &[u8]) -> StorageResult<()> {
        self.WriteData(buffer, buffer.len())
    }

    // Writes a type into the stream and moves the stream forward sizeof(T)
    // bytes.
    fn Write<T: Copy>(&mut self, element: T) -> StorageResult<()> {
        let ptr = &element as *const T as *const u8;
        let bytes = unsafe { std::slice::from_raw_parts(ptr, size_of::<T>()) };
        self.WriteData(bytes, bytes.len())
    }
}
