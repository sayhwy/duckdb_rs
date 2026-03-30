// ============================================================
// file_buffer.rs — 内存缓冲区
// 对应 C++: duckdb/common/file_buffer.hpp / file_buffer.cpp
// ============================================================

use super::types::{DEFAULT_BLOCK_ALLOC_SIZE, DEFAULT_BLOCK_HEADER_SIZE, FileBufferType};

/// 对应 C++ FileBuffer
///
/// 持有实际字节数据，分为 header 区和 payload 区：
///   [ header_size bytes | block_size bytes ]
///
/// C++ 中 FileBuffer 通过裸指针（buffer = InternalBuffer()）暴露 payload；
/// Rust 中通过方法返回切片，消除指针运算。
pub struct FileBuffer {
    buffer_type: FileBufferType,
    /// 含 header 的完整分配数据
    data: Vec<u8>,
    /// header 占用字节数（payload 从 data[header_size..] 开始）
    header_size: usize,
}

impl FileBuffer {
    /// 分配新的 FileBuffer。
    /// `alloc_size` 是含 header 的总大小（对应 C++ AllocSize()）。
    pub fn new(buffer_type: FileBufferType, alloc_size: usize, header_size: usize) -> Self {
        debug_assert!(
            alloc_size >= header_size,
            "alloc_size must be >= header_size"
        );
        Self {
            buffer_type,
            data: vec![0u8; alloc_size],
            header_size,
        }
    }

    /// 创建标准 Block 缓冲区（使用默认分配大小和 header 大小）
    pub fn new_block() -> Self {
        Self::new(
            FileBufferType::Block,
            DEFAULT_BLOCK_ALLOC_SIZE,
            DEFAULT_BLOCK_HEADER_SIZE,
        )
    }

    // ─── 属性访问 ────────────────────────────────────────────

    pub fn buffer_type(&self) -> FileBufferType {
        self.buffer_type
    }

    /// 含 header 的总分配大小（对应 C++ AllocSize()）
    pub fn alloc_size(&self) -> usize {
        self.data.len()
    }

    pub fn header_size(&self) -> usize {
        self.header_size
    }

    /// payload 大小（不含 header），对应 C++ GetBufferSize()
    pub fn payload_size(&self) -> usize {
        self.data.len().saturating_sub(self.header_size)
    }

    // ─── 数据访问 ────────────────────────────────────────────

    /// header 区只读切片
    pub fn header(&self) -> &[u8] {
        &self.data[..self.header_size]
    }

    /// header 区可写切片
    pub fn header_mut(&mut self) -> &mut [u8] {
        &mut self.data[..self.header_size]
    }

    /// payload 区只读切片（对应 C++ InternalBuffer() const）
    pub fn payload(&self) -> &[u8] {
        &self.data[self.header_size..]
    }

    /// payload 区可写切片（对应 C++ InternalBuffer()）
    pub fn payload_mut(&mut self) -> &mut [u8] {
        &mut self.data[self.header_size..]
    }

    /// 完整数据只读切片（含 header）
    pub fn raw(&self) -> &[u8] {
        &self.data
    }

    /// 完整数据可写切片（含 header）
    pub fn raw_mut(&mut self) -> &mut [u8] {
        &mut self.data
    }

    // ─── 变形操作 ────────────────────────────────────────────

    /// 调整 payload 大小（对应 C++ Resize()）
    /// 保留 header，重新分配 payload 部分。
    pub fn resize(&mut self, new_payload_size: usize) {
        let new_alloc = self.header_size + new_payload_size;
        self.data.resize(new_alloc, 0);
    }

    /// 将 src 的 payload 数据复制进来（对应 memcpy InternalBuffer）
    pub fn copy_from(&mut self, src: &[u8]) {
        let payload = self.payload_mut();
        let len = payload.len().min(src.len());
        payload[..len].copy_from_slice(&src[..len]);
    }
}
