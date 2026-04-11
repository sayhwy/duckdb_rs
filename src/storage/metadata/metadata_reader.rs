// ============================================================
// metadata/metadata_reader.rs — 元数据顺序读取器
// 对应 C++: duckdb/storage/metadata/metadata_reader.hpp + .cpp
// ============================================================
//
// 核心职责：
//   实现 ReadStream trait，从链式 meta 子块中顺序读取字节流。
//   可跨越多个存储块，按 MetaBlockPointer 链表跳转。
//
// 子块内存布局（每个 meta 子块 = metadata_block_size 字节）：
//   [0..8]     : 下一个 MetaBlockPointer（u64；0xFFFF...FF = 链表结束）
//   [8..size]  : 实际数据
//
// C++ 指针访问 → Rust 缓冲区拷贝：
//   C++ 使用 BasePtr() + offset 的裸指针直接读取。
//   Rust 在 ReadNextBlock() 时把当前子块的数据拷贝到 current_data: Vec<u8>，
//   read_data() 从 Vec 中 memcpy，跨块时调用 ReadNextBlock 换入新 Vec。
//   这消除了原始指针，代价是每次换块时多一次拷贝（metadata 块小，可接受）。
//
// BlockReaderType：
//   ExistingBlocks — from_disk_pointer（块必须已注册）
//   RegisterBlocks — register_disk_pointer（若未注册则自动注册）
//
// ============================================================

use super::metadata_manager::MetadataManager;
use super::types::{MetaBlockPointer, MetadataHandle, ReadStream};

// ─── BlockReaderType ──────────────────────────────────────────

/// 对应 C++ enum class BlockReaderType
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlockReaderType {
    /// 要求块已注册（对应 EXISTING_BLOCKS）
    ExistingBlocks,
    /// 若未注册则自动注册（对应 REGISTER_BLOCKS）
    RegisterBlocks,
}

// ─── MetadataReader ───────────────────────────────────────────

/// 对应 C++ MetadataReader : public ReadStream
///
/// 从链式 meta 子块链表中顺序读取字节流。
/// 生命周期绑定到 MetadataManager（`'mgr`）。
pub struct MetadataReader<'mgr> {
    /// 对应 C++ MetadataManager &manager
    manager: &'mgr MetadataManager,

    /// 对应 C++ BlockReaderType type
    reader_type: BlockReaderType,

    /// 当前 pin 住的元数据 handle（对应 C++ MetadataHandle block）
    /// None 表示尚未读取第一块
    current_handle: Option<MetadataHandle>,

    /// 当前子块的数据副本（对应 C++ 中 BasePtr() 指向的内存区域）
    /// 长度 = metadata_block_size；第 0..8 字节为 next-pointer（已解析后保留供诊断）
    current_data: Vec<u8>,

    /// 下一块的磁盘指针（对应 C++ next_pointer）
    next_pointer: MetaBlockPointer,

    /// 是否还有下一块（对应 C++ has_next_block）
    has_next_block: bool,

    /// 记录已读取块列表（对应 C++ optional_ptr<vector<MetaBlockPointer>> read_pointers）
    read_pointers: Option<Vec<MetaBlockPointer>>,

    /// 当前子块内的读取位置（字节偏移，相对于子块起始；初始 = 8，跳过 next-pointer 字段）
    offset: usize,

    /// 首次读取的初始偏移（对应 C++ next_offset，构造时由 pointer.offset 决定）
    initial_offset: usize,

    /// 子块容量（= metadata_block_size；0 表示尚未初始化）
    capacity: usize,

    /// 是否已到达数据末尾（无更多块且当前块已读完）
    pub hit_eof: bool,
}

impl<'mgr> MetadataReader<'mgr> {
    // ─── 构造 ─────────────────────────────────────────────────

    /// 对应 C++ MetadataReader(MetadataManager&, MetaBlockPointer, optional read_pointers, BlockReaderType)
    pub fn new(
        manager: &'mgr MetadataManager,
        pointer: MetaBlockPointer,
        read_pointers: Option<Vec<MetaBlockPointer>>,
        reader_type: BlockReaderType,
    ) -> Self {
        let initial_offset = pointer.offset as usize;
        Self {
            manager,
            reader_type,
            current_handle: None,
            current_data: Vec::new(),
            next_pointer: pointer,
            has_next_block: true,
            read_pointers,
            offset: 0,
            initial_offset,
            capacity: 0,
            hit_eof: false,
        }
    }

    /// 对应 C++ MetadataReader(MetadataManager&, BlockPointer)
    pub fn from_block_pointer(
        manager: &'mgr MetadataManager,
        pointer: super::types::BlockPointer,
    ) -> Self {
        let meta_pointer =
            MetadataManager::from_block_pointer(pointer, manager.get_metadata_block_size());
        Self::new(manager, meta_pointer, None, BlockReaderType::ExistingBlocks)
    }

    // ─── 指针查询 ─────────────────────────────────────────────

    /// 对应 C++ MetadataReader::GetMetaBlockPointer()
    pub fn get_meta_block_pointer(&self) -> MetaBlockPointer {
        assert!(
            self.capacity > 0,
            "GetMetaBlockPointer called before first read"
        );
        self.manager.get_disk_pointer(
            &self.current_handle.as_ref().unwrap().pointer,
            self.offset as u32,
        )
    }

    pub fn take_read_pointers(&mut self) -> Option<Vec<MetaBlockPointer>> {
        self.read_pointers.take()
    }

    /// 对应 C++ MetadataReader::GetRemainingBlocks(MetaBlockPointer last_block)
    ///
    /// 消耗剩余所有块，返回它们的 MetaBlockPointer 列表。
    /// 若指定 last_block（is_valid()），到达该块时停止。
    pub fn get_remaining_blocks(&mut self, last_block: MetaBlockPointer) -> Vec<MetaBlockPointer> {
        let mut result = Vec::new();
        while self.has_next_block {
            if last_block.is_valid() && self.next_pointer.block_pointer == last_block.block_pointer
            {
                break;
            }
            result.push(self.next_pointer);
            self.read_next_block();
        }
        result
    }

    // ─── 私有：块切换 ─────────────────────────────────────────

    /// 将 next_pointer 解析为内存 MetadataPointer（根据 reader_type 选择注册方式）
    fn from_disk_pointer_internal(
        &self,
        pointer: MetaBlockPointer,
    ) -> super::types::MetadataPointer {
        match self.reader_type {
            BlockReaderType::ExistingBlocks => self.manager.from_disk_pointer(pointer),
            BlockReaderType::RegisterBlocks => self.manager.register_disk_pointer(pointer),
        }
    }

    /// 对应 C++ MetadataReader::ReadNextBlock()
    ///
    /// Pin 下一块，解析 next-pointer 字段，将子块数据拷贝到 current_data。
    fn read_next_block(&mut self) {
        assert!(self.has_next_block, "No more data in MetadataReader");

        // 记录 read_pointers（若启用）
        if let Some(ref mut ptrs) = self.read_pointers {
            ptrs.push(self.next_pointer);
        }

        // 解析 next_pointer → MetadataPointer，然后 Pin
        let mem_pointer = self.from_disk_pointer_internal(self.next_pointer);
        let handle = self.manager.pin(&mem_pointer);
        let sub_index = mem_pointer.index as usize;
        let metadata_block_size = self.manager.get_metadata_block_size();

        // 从 BufferHandle 拷贝当前子块数据
        // 对应 C++: BasePtr() = handle.Ptr() + index * metadata_block_size
        let data_slice: Vec<u8> = handle
            .handle
            .with_data(|payload| {
                let start = sub_index * metadata_block_size;
                let end = (start + metadata_block_size).min(payload.len());
                if start < payload.len() {
                    payload[start..end].to_vec()
                } else {
                    Vec::new()
                }
            })
            .unwrap_or_default();

        // 解析 next-pointer（前 8 字节）
        // 对应 C++: idx_t next_block = Load<idx_t>(BasePtr())
        let next_block_raw = if data_slice.len() >= 8 {
            u64::from_le_bytes(data_slice[0..8].try_into().unwrap())
        } else {
            u64::MAX
        };

        if next_block_raw == u64::MAX {
            self.has_next_block = false;
        } else {
            self.next_pointer = MetaBlockPointer {
                block_pointer: next_block_raw,
                offset: 0,
            };
        }

        // 设定读取起始偏移
        // 第一块使用 initial_offset；后续块从字节 8 开始
        let start_offset = if self.capacity == 0 {
            // 第一块：使用构造时传入的 pointer.offset，最小为 8
            self.initial_offset.max(std::mem::size_of::<u64>())
        } else {
            std::mem::size_of::<u64>() // 跳过 next-pointer 字段
        };

        self.current_handle = Some(handle);
        self.current_data = data_slice;
        self.offset = start_offset;
        self.capacity = metadata_block_size;
    }
}

// ─── ReadStream 实现 ──────────────────────────────────────────

impl<'mgr> ReadStream for MetadataReader<'mgr> {
    /// 对应 C++ MetadataReader::ReadData(data_ptr_t buffer, idx_t read_size)
    ///
    /// 支持跨越多个 meta 子块的读取（每次跨块时调用 read_next_block）。
    fn is_eof(&self) -> bool {
        self.hit_eof
    }

    fn read_data(&mut self, buf: &mut [u8]) {
        // 已达 EOF：填充 0xFF 使上层 BinaryDeserializer 读到 0xFFFF 并终止对象
        if self.hit_eof {
            buf.fill(0xFF);
            return;
        }

        let mut read_size = buf.len();
        let mut buffer_offset = 0usize;

        // 对应 C++ 的逻辑：while (offset + read_size > capacity)
        while self.offset + read_size > self.capacity {
            // 无法从当前块读取全部数据
            // 先读取当前块的剩余部分
            let to_read = self.capacity - self.offset;
            if to_read > 0 {
                let src_end = self.offset + to_read;
                buf[buffer_offset..buffer_offset + to_read]
                    .copy_from_slice(&self.current_data[self.offset..src_end]);
                read_size -= to_read;
                buffer_offset += to_read;
                self.offset += to_read;
            }
            // 然后移动到下一块
            if !self.has_next_block {
                // 没有更多块：填充 0xFF（BinarySerializer 0xFFFF 终止符），避免 panic
                // 这会使上层 BinaryDeserializer 在下次读 field_id 时得到 0xFFFF 并自然终止对象
                self.hit_eof = true;
                for b in &mut buf[buffer_offset..buffer_offset + read_size] {
                    *b = 0xFF;
                }
                return;
            }
            self.read_next_block();
        }
        // 当前块有足够空间读取剩余数据
        let src_end = self.offset + read_size;
        buf[buffer_offset..buffer_offset + read_size]
            .copy_from_slice(&self.current_data[self.offset..src_end]);
        self.offset += read_size;
    }
}
