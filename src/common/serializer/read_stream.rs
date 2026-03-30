#![allow(non_snake_case)]

use std::mem::{MaybeUninit, size_of};

pub use crate::storage::checkpoint::QueryContext;
use crate::storage::storage_info::StorageResult;

pub trait ReadStream {
    // Reads a set amount of data from the stream into the specified buffer and
    // moves the stream forward accordingly.
    fn ReadData(&mut self, buffer: &mut [u8], read_size: usize) -> StorageResult<()>;
    fn ReadDataWithContext(
        &mut self,
        context: QueryContext,
        buffer: &mut [u8],
        read_size: usize,
    ) -> StorageResult<()>;

    fn read_data(&mut self, buffer: &mut [u8]) -> StorageResult<()> {
        self.ReadData(buffer, buffer.len())
    }

    fn read_data_with_context(
        &mut self,
        context: QueryContext,
        buffer: &mut [u8],
    ) -> StorageResult<()> {
        self.ReadDataWithContext(context, buffer, buffer.len())
    }

    // Reads a type from the stream and moves the stream forward sizeof(T) bytes.
    fn Read<T: Copy>(&mut self) -> StorageResult<T> {
        self.ReadWithContext(QueryContext::default())
    }

    fn ReadWithContext<T: Copy>(&mut self, context: QueryContext) -> StorageResult<T> {
        let mut value = MaybeUninit::<T>::uninit();
        let ptr = value.as_mut_ptr() as *mut u8;
        let bytes = unsafe { std::slice::from_raw_parts_mut(ptr, size_of::<T>()) };
        self.ReadDataWithContext(context, bytes, bytes.len())?;
        Ok(unsafe { value.assume_init() })
    }
}
