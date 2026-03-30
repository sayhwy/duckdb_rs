// ============================================================
// single_file_block_manager.rs — 单文件块管理器
// 对应 C++: duckdb/storage/single_file_block_manager.hpp
//           duckdb/storage/single_file_block_manager.cpp
// ============================================================
//
// 核心职责：
//   将逻辑 BlockId 映射到单个数据库文件中的物理偏移量，
//   维护空闲块列表（free list），执行块级 I/O，
//   在 checkpoint 时交替写入两个头部槽。
//
// 块偏移计算（对应 C++ GetBlockLocation）：
//   offset = 2 * FILE_HEADER_SIZE + block_id * block_alloc_size
//
// 头部交替写入（对应 C++ WriteHeader(uint64_t iteration)）：
//   slot = iteration % 2；写完后 iteration_count++
//
// ============================================================

use parking_lot::Mutex;
use std::any::Any;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::{Arc, OnceLock, Weak};

use crate::storage::buffer::{
    BlockHandle, BlockId, BlockManager, BufferManager, BufferPool, FileBuffer, FileBufferType,
    INVALID_BLOCK, MemoryTag,
};

use super::checksum::checksum;
use super::metadata::{
    MetadataHandle, MetadataManager, WriteStream, free_list_block_writer::FreeListBlockWriter,
};
use super::storage_info::{
    BLOCK_HEADER_SIZE, DEFAULT_BLOCK_ALLOC_SIZE, DEFAULT_BLOCK_HEADER_STORAGE_SIZE, DatabaseHeader,
    FILE_HEADER_SIZE, FileHandle, FileOpenFlags, FileSystem, MAGIC_BYTES, MainHeader,
    StorageManagerOptions, StorageResult,
};

// ─── BlockListState（受 Mutex 保护）──────────────────────────

/// 所有受 block_list_lock_ 保护的块分配状态
struct BlockListState {
    /// 对应 C++ free_list_（有序，保证 checkpoint 顺序分配）
    free_list: BTreeSet<BlockId>,
    /// 对应 C++ free_blocks_in_use_（已释放但仍有内存引用）
    free_blocks_in_use: BTreeSet<BlockId>,
    /// 对应 C++ newly_used_blocks_（上次 checkpoint 后新分配）
    newly_used_blocks: BTreeSet<BlockId>,
    /// 对应 C++ multi_use_blocks_（引用计数 > 1 的块）
    multi_use_blocks: HashMap<BlockId, u32>,
    /// 对应 C++ modified_blocks_（已修改，下次 checkpoint 后可释放）
    modified_blocks: HashSet<BlockId>,
    /// 对应 C++ max_block_（文件末尾下一个块的 ID）
    max_block: BlockId,
    /// 对应 C++ iteration_count_
    iteration_count: u64,
}

impl BlockListState {
    fn new() -> Self {
        Self {
            free_list: BTreeSet::new(),
            free_blocks_in_use: BTreeSet::new(),
            newly_used_blocks: BTreeSet::new(),
            multi_use_blocks: HashMap::new(),
            modified_blocks: HashSet::new(),
            // DuckDB C++ starts max_block at 0 for new databases
            // block_id 0 is the first DATA block (after the 3 file headers)
            // The file headers are NOT counted as blocks - they are file overhead
            max_block: 0,
            iteration_count: 0,
        }
    }

    /// 对应 C++ GetFreeBlockIdInternal()
    fn get_free_block_id(&mut self) -> BlockId {
        if let Some(&id) = self.free_list.iter().next() {
            self.free_list.remove(&id);
            self.newly_used_blocks.insert(id);
            id
        } else {
            let id = self.max_block;
            self.max_block += 1;
            self.newly_used_blocks.insert(id);
            id
        }
    }

    /// 对应 C++ MarkBlockAsModified()
    fn mark_as_modified(&mut self, block_id: BlockId) {
        if let Some(count) = self.multi_use_blocks.get_mut(&block_id) {
            *count -= 1;
            if *count == 0 {
                self.multi_use_blocks.remove(&block_id);
            } else {
                return; // 仍有其他引用
            }
        }
        self.modified_blocks.insert(block_id);
    }

    /// checkpoint 完成后将上轮 modified_blocks 移入 free_list
    fn sweep_modified_to_free(&mut self) {
        for id in self.modified_blocks.drain() {
            self.free_list.insert(id);
        }
    }
}

// ─── BlockMap 状态 ────────────────────────────────────────────

/// 已注册的 BlockHandle 弱引用表
struct BlockMap {
    blocks: HashMap<BlockId, Weak<BlockHandle>>,
}

impl BlockMap {
    fn new() -> Self {
        Self {
            blocks: HashMap::new(),
        }
    }
}

// ─── SingleFileBlockManager ──────────────────────────────────

/// 对应 C++ SingleFileBlockManager
pub struct SingleFileBlockManager {
    /// 关联的缓冲区管理器
    buffer_manager: Arc<dyn BufferManager>,
    /// 文件系统实现
    fs: Arc<dyn FileSystem>,
    /// 数据库文件路径
    path: String,

    /// 块分配状态（受 Mutex 保护）
    block_state: Mutex<BlockListState>,
    /// 已注册 BlockHandle 的弱引用表（受 Mutex 保护）
    block_map: Mutex<BlockMap>,

    /// 两个头部 DatabaseHeader（对应 C++ header_[2]）
    headers: Mutex<[DatabaseHeader; 2]>,
    /// 两个头部缓冲区（对应 C++ header_buffer_[2]）
    header_buffers: Mutex<[Box<FileBuffer>; 2]>,

    /// 元数据管理器（对应 C++ MetadataManager）
    metadata_manager: OnceLock<Arc<MetadataManager>>,

    /// 每个 block 开头的元数据大小（对应 C++ block_header_size_）
    block_header_size: usize,
    /// 含头部的完整块分配大小（对应 C++ block_alloc_size_）
    block_alloc_size: usize,

    read_only: bool,
    use_direct_io: bool,
    self_ref: OnceLock<Weak<dyn BlockManager>>,
}

impl SingleFileBlockManager {
    pub fn new(
        buffer_manager: Arc<dyn BufferManager>,
        fs: Arc<dyn FileSystem>,
        path: String,
        opts: &StorageManagerOptions,
    ) -> Arc<Self> {
        let block_alloc_size = if opts.block_alloc_size == 0 {
            DEFAULT_BLOCK_ALLOC_SIZE
        } else {
            opts.block_alloc_size
        };
        let manager = Arc::new(Self {
            buffer_manager: buffer_manager.clone(),
            fs,
            path,
            block_state: Mutex::new(BlockListState::new()),
            block_map: Mutex::new(BlockMap::new()),
            headers: Mutex::new([DatabaseHeader::default(), DatabaseHeader::default()]),
            header_buffers: Mutex::new([
                Box::new(FileBuffer::new(
                    FileBufferType::ManagedBuffer,
                    FILE_HEADER_SIZE,
                    BLOCK_HEADER_SIZE,
                )),
                Box::new(FileBuffer::new(
                    FileBufferType::ManagedBuffer,
                    FILE_HEADER_SIZE,
                    BLOCK_HEADER_SIZE,
                )),
            ]),
            metadata_manager: OnceLock::new(),
            block_header_size: BLOCK_HEADER_SIZE,
            block_alloc_size,
            read_only: opts.read_only,
            use_direct_io: opts.use_direct_io,
            self_ref: OnceLock::new(),
        });
        let dyn_manager: Arc<dyn BlockManager> = manager.clone();
        let _ = manager.self_ref.set(Arc::downgrade(&dyn_manager));
        manager
    }

    // ─── 文件偏移 ─────────────────────────────────────────────

    /// 对应 C++ GetBlockLocation()
    ///
    /// 在 DuckDB 中，文件布局如下：
    /// - 0 ~ 3*FILE_HEADER_SIZE: 文件头（MainHeader + 2个DatabaseHeader），每个 FILE_HEADER_SIZE 字节
    /// - 3*FILE_HEADER_SIZE+: 数据块，每个 block_alloc_size 字节
    ///
    /// 数据块的 block_id 从 0 开始，第一个数据块在偏移 3*FILE_HEADER_SIZE。
    /// 注意：文件头不是数据块，不计入 block_id！
    pub fn block_offset(&self, block_id: BlockId) -> u64 {
        debug_assert!(block_id >= 0);
        // 数据块从 3 个文件头之后开始
        3 * super::storage_info::FILE_HEADER_SIZE as u64
            + block_id as u64 * self.block_alloc_size as u64
    }

    // ─── 初始化 ───────────────────────────────────────────────

    /// 对应 C++ SingleFileBlockManager::Initialize()
    pub fn initialize(&self) -> StorageResult<()> {
        let flags = self.open_flags();
        let mut file = self.fs.open_file(&self.path, flags)?;

        if file.file_size()? == 0 {
            self.write_initial_header(&mut *file)
        } else {
            self.read_and_verify_header(&mut *file)?;
            // 对应 C++ LoadExistingDatabase()：只读模式下跳过 free list 加载
            if !self.read_only {
                self.load_free_list(&mut *file)?;
            }
            Ok(())
        }
    }

    fn open_flags(&self) -> FileOpenFlags {
        let mut flags = if self.read_only {
            FileOpenFlags::READ
        } else {
            FileOpenFlags::READ_WRITE | FileOpenFlags::CREATE
        };
        if self.use_direct_io {
            flags = flags | FileOpenFlags::DIRECT_IO;
        }
        flags
    }

    fn write_initial_header(&self, file: &mut dyn FileHandle) -> StorageResult<()> {
        let mut hdr_bufs = self.header_buffers.lock();

        // DuckDB file layout:
        // - Block 0: MainHeader at offset 0, FILE_HEADER_SIZE bytes
        // - Block 1: DatabaseHeader 0 at offset FILE_HEADER_SIZE
        // - Block 2: DatabaseHeader 1 at offset 2 * FILE_HEADER_SIZE
        // - Data blocks start at offset 3 * FILE_HEADER_SIZE

        // MainHeader at offset 0
        let main = MainHeader::new(super::storage_info::VERSION_NUMBER);
        main.serialize(hdr_bufs[0].payload_mut());

        let checksum_val = checksum(hdr_bufs[0].payload());
        hdr_bufs[0].raw_mut()[0..8].copy_from_slice(&checksum_val.to_le_bytes());
        file.write_at(hdr_bufs[0].raw(), 0)?;

        // DatabaseHeaders at FILE_HEADER_SIZE and 2*FILE_HEADER_SIZE
        // block_count = 0 because no data blocks exist yet (matches C++ behavior)
        let db_hdr = DatabaseHeader {
            iteration: 0,
            meta_block: INVALID_BLOCK,
            free_list: INVALID_BLOCK,
            block_count: 0, // No data blocks yet
            block_alloc_size: self.block_alloc_size as u64,
            vector_size: 2048,
            serialization_compatibility: 7,
        };
        db_hdr.serialize(hdr_bufs[1].payload_mut());

        let checksum_val = checksum(hdr_bufs[1].payload());
        hdr_bufs[1].raw_mut()[0..8].copy_from_slice(&checksum_val.to_le_bytes());
        file.write_at(
            hdr_bufs[1].raw(),
            super::storage_info::FILE_HEADER_SIZE as u64,
        )?;

        // Second DatabaseHeader at 2*FILE_HEADER_SIZE
        file.write_at(
            hdr_bufs[1].raw(),
            2 * super::storage_info::FILE_HEADER_SIZE as u64,
        )?;

        file.sync()
    }

    fn read_and_verify_header(&self, file: &mut dyn FileHandle) -> StorageResult<()> {
        let mut hdr_bufs = self.header_buffers.lock();

        // Read MainHeader at offset 0
        file.read_at(hdr_bufs[0].raw_mut(), 0)?;

        // Read DatabaseHeader 0 at offset FILE_HEADER_SIZE
        file.read_at(
            hdr_bufs[1].raw_mut(),
            super::storage_info::FILE_HEADER_SIZE as u64,
        )?;

        // Verify MainHeader magic bytes
        MainHeader::deserialize(
            &hdr_bufs[0].raw()[BLOCK_HEADER_SIZE..BLOCK_HEADER_SIZE + MainHeader::SERIALIZED_SIZE],
        )
        .or_else(|| {
            MainHeader::deserialize(
                &hdr_bufs[1].raw()
                    [BLOCK_HEADER_SIZE..BLOCK_HEADER_SIZE + MainHeader::SERIALIZED_SIZE],
            )
        })
        .ok_or_else(|| super::storage_info::StorageError::Corrupt {
            msg: "Invalid magic bytes — not a DuckDB file".into(),
        })?;

        // Read DatabaseHeader 0
        let h0 = DatabaseHeader::deserialize(&hdr_bufs[1].raw()[BLOCK_HEADER_SIZE..]);

        // Read DatabaseHeader 1 at offset 2*FILE_HEADER_SIZE
        file.read_at(
            hdr_bufs[1].raw_mut(),
            2 * super::storage_info::FILE_HEADER_SIZE as u64,
        )?;
        let h1 = DatabaseHeader::deserialize(&hdr_bufs[1].raw()[BLOCK_HEADER_SIZE..]);

        let mut headers = self.headers.lock();
        *headers = [h0, h1];

        let active_idx = MainHeader::active_header_idx(&*headers);
        let mut state = self.block_state.lock();
        state.iteration_count = headers[active_idx].iteration;
        // block_count stores the next data block ID to allocate
        state.max_block = headers[active_idx].block_count as BlockId;
        Ok(())
    }

    /// 对应 C++ LoadFreeList()
    fn load_free_list(&self, file: &mut dyn FileHandle) -> StorageResult<()> {
        let active_free_list_packed = {
            let headers = self.headers.lock();
            let state = self.block_state.lock();
            let slot = (state.iteration_count % 2) as usize;
            headers[slot].free_list
        };

        if active_free_list_packed == INVALID_BLOCK {
            return Ok(());
        }

        // free_list 字段存储的是打包的 MetaBlockPointer（idx_t）：
        //   bits[63:56] = block_index（物理块内第几个 meta 子块，0..63）
        //   bits[55:0]  = block_id（物理数据块编号）
        // 对应 C++ MetaBlockPointer ptr(GetActiveHeader().free_list, 0)
        let metadata_block_size = (self.block_alloc_size - self.block_header_size) / 64;
        let mut sub_buf = vec![0u8; metadata_block_size];
        let mut current_packed = active_free_list_packed as u64;

        let mut free_ids: Vec<i64> = Vec::new();
        let mut total_count: Option<usize> = None;

        loop {
            // 解包 MetaBlockPointer
            let block_id = current_packed & 0x00FFFFFFFFFFFFFF;
            let block_index = (current_packed >> 56) as usize;

            // 计算子块在文件中的偏移：
            //   物理块起始 + 8字节块头 + block_index * metadata_block_size
            // Blocks 0, 1, 2 are headers, so data blocks start at block 3
            let block_file_offset = self.block_offset(block_id as BlockId);
            let sub_block_offset = block_file_offset
                + self.block_header_size as u64
                + (block_index * metadata_block_size) as u64;

            file.read_at(&mut sub_buf, sub_block_offset)?;

            // 子块前 8 字节：下一个 MetaBlockPointer（i64 LE，-1 表示链表末尾）
            let next_packed = i64::from_le_bytes(sub_buf[0..8].try_into().unwrap());
            let data = &sub_buf[8..];

            if total_count.is_none() {
                // 首个子块：先读 count
                let count = u64::from_le_bytes(data[0..8].try_into().unwrap()) as usize;
                total_count = Some(count);
                let avail = (metadata_block_size - 8 - 8) / 8;
                for i in 0..count.min(avail) {
                    let off = 8 + i * 8;
                    let bid = i64::from_le_bytes(data[off..off + 8].try_into().unwrap());
                    free_ids.push(bid);
                }
            } else {
                // 续链子块：继续读 block_id
                let remaining = total_count.unwrap().saturating_sub(free_ids.len());
                let avail = (metadata_block_size - 8) / 8;
                for i in 0..remaining.min(avail) {
                    let off = i * 8;
                    let bid = i64::from_le_bytes(data[off..off + 8].try_into().unwrap());
                    free_ids.push(bid);
                }
            }

            if next_packed == -1 || total_count.map_or(true, |c| free_ids.len() >= c) {
                break;
            }
            current_packed = next_packed as u64;
        }

        let mut state = self.block_state.lock();
        for bid in free_ids {
            state.free_list.insert(bid);
        }
        Ok(())
    }

    // ─── 头部 Checkpoint ──────────────────────────────────────

    /// 对应 C++ WriteHeader(uint64_t iteration, DatabaseHeader &header)
    pub fn write_header(&self, mut new_header: DatabaseHeader) -> StorageResult<()> {
        debug_assert!(!self.read_only);

        // Increment iteration and set it in the header (matches C++ behavior)
        let (slot, iteration) = {
            let mut state = self.block_state.lock();
            state.iteration_count += 1;
            // C++ writes to the slot OPPOSITE to the current active header
            // slot = (active_header == 1) ? 0 : 1
            // But we use iteration_count % 2 which achieves the same alternating behavior
            let slot = (state.iteration_count % 2) as usize;
            (slot, state.iteration_count)
        };

        new_header.iteration = iteration;

        let mut file = self.fs.open_file(&self.path, self.open_flags())?;
        let mut hdr_bufs = self.header_buffers.lock();
        let mut headers = self.headers.lock();
        headers[slot] = new_header;

        // Write only the DatabaseHeader for this slot; the MainHeader is never
        // rewritten after initial creation (matches C++ WriteHeader behaviour).
        headers[slot].serialize(hdr_bufs[slot].payload_mut());

        // Calculate checksum
        let checksum_val = checksum(hdr_bufs[slot].payload());
        hdr_bufs[slot].raw_mut()[0..8].copy_from_slice(&checksum_val.to_le_bytes());

        // Write at FILE_HEADER_SIZE * (1 + slot)
        let offset = (1 + slot) as u64 * super::storage_info::FILE_HEADER_SIZE as u64;
        file.write_at(hdr_bufs[slot].raw(), offset)?;
        file.sync()?;

        Ok(())
    }

    // ─── 公开块管理接口 ───────────────────────────────────────

    pub fn get_free_block_id(&self) -> BlockId {
        self.block_state.lock().get_free_block_id()
    }

    pub fn mark_block_as_modified(&self, block_id: BlockId) {
        self.block_state.lock().mark_as_modified(block_id);
    }

    pub fn increase_block_ref_count(&self, block_id: BlockId) {
        let mut state = self.block_state.lock();
        *state.multi_use_blocks.entry(block_id).or_insert(1) += 1;
    }

    pub fn block_count(&self) -> u64 {
        self.block_state.lock().max_block as u64
    }

    pub fn meta_block_size(&self) -> usize {
        self.block_alloc_size - self.block_header_size
    }

    pub fn finalize_checkpoint(&self) {
        self.block_state.lock().sweep_modified_to_free();
    }

    /// 设置 MetadataManager（在创建后调用，只能设置一次）
    pub fn set_metadata_manager(&self, metadata_manager: Arc<MetadataManager>) {
        let _ = self.metadata_manager.set(metadata_manager);
    }

    /// 获取 MetadataManager 引用
    pub fn get_metadata_manager(&self) -> Option<&Arc<MetadataManager>> {
        self.metadata_manager.get()
    }

    // ─── Free List 持久化 ─────────────────────────────────────

    /// 对应 C++ GetFreeListBlocks()
    ///
    /// 预分配足够的块来存储 free list 数据，避免在写入时从 free list 中分配（循环依赖）
    fn get_free_list_blocks(&self) -> Vec<MetadataHandle> {
        let metadata_manager = match self.metadata_manager.get() {
            Some(mgr) => mgr,
            None => return Vec::new(),
        };

        let metadata_block_size = metadata_manager.get_metadata_block_size();
        // 每个 free_list_block 的可用容量（去掉 8 字节链表 next-pointer）
        let block_capacity = metadata_block_size - std::mem::size_of::<u64>();

        // 固定点迭代：每次分配新块后 block_count 增加，需重新计算所需大小。
        // 每次迭代净增 (block_capacity - 16) 字节有效空间，因此收敛极快（通常 1-2 次）。
        //
        // C++ DuckDB: SingleFileBlockManager::GetFreeListBlocks() 采用同样的迭代策略
        let mut free_list_blocks: Vec<MetadataHandle> = Vec::new();

        loop {
            // 重新计算所需大小（block_count 可能因上次迭代的分配而增大）
            //
            // all_free_blocks（写入 free list 的块集合）= free_list ∪ modified_blocks ∪ newly_used_blocks
            // 需要包含所有三个来源，否则会低估 free_list_size。
            // C++ DuckDB GetFreeListBlocks() 采用同样的固定点迭代 + 完整计数策略。
            let (all_free_count, multi_use_blocks_len, current_block_count) = {
                let state = self.block_state.lock();
                // 用 BTreeSet 模拟 all_free_blocks 的合并去重结果大小
                // （实际 write_header_with_free_list 也会合并这三个集合）
                let mut merged_count = state.free_list.len();
                for id in &state.modified_blocks {
                    if !state.free_list.contains(id) {
                        merged_count += 1;
                    }
                }
                for id in &state.newly_used_blocks {
                    if !state.free_list.contains(id) && !state.modified_blocks.contains(id) {
                        merged_count += 1;
                    }
                }
                (
                    merged_count,
                    state.multi_use_blocks.len(),
                    metadata_manager.block_count(),
                )
            };

            // 1. free_list: count(u64) + block_ids(u64 each)
            let free_list_size =
                std::mem::size_of::<u64>() + std::mem::size_of::<BlockId>() * all_free_count;

            // 2. multi_use_blocks: count(u64) + (block_id: u64, ref_count: u32) pairs
            let multi_use_blocks_size = std::mem::size_of::<u64>()
                + (std::mem::size_of::<BlockId>() + std::mem::size_of::<u32>())
                    * multi_use_blocks_len;

            // 3. metadata blocks: count(u64) + (block_id: u64, free_mask: u64) pairs
            // current_block_count 包含本轮已分配的 free_list_blocks
            let metadata_blocks_size = std::mem::size_of::<u64>()
                + (std::mem::size_of::<BlockId>() + std::mem::size_of::<u64>())
                    * current_block_count;

            let total_size = free_list_size + multi_use_blocks_size + metadata_blocks_size;
            let already_allocated = free_list_blocks.len() * block_capacity;

            if already_allocated >= total_size {
                // 已分配足够空间，退出迭代
                break;
            }

            // 需要更多空间：再分配一个块（会使 block_count 加 1）
            let handle = metadata_manager.allocate_handle();
            free_list_blocks.push(handle);
        }

        free_list_blocks
    }

    /// 对应 C++ WriteHeader() 的完整实现
    ///
    /// 包括：
    /// 1. 预分配 free list blocks
    /// 2. 标记修改的元数据块
    /// 3. 收集所有空闲块
    /// 4. 写入 free list 数据
    /// 5. 刷盘并写入 DatabaseHeader
    pub fn write_header_with_free_list(&self, mut header: DatabaseHeader) -> StorageResult<()> {
        debug_assert!(!self.read_only);

        let metadata_manager = match self.metadata_manager.get() {
            Some(mgr) => mgr,
            None => {
                // 如果没有 MetadataManager，回退到简单的 write_header
                return self.write_header(header);
            }
        };

        // 1. 预分配 free list blocks
        let free_list_blocks = self.get_free_list_blocks();

        // 2. 标记修改的元数据块
        metadata_manager.mark_blocks_as_modified();

        // 3. 收集所有空闲块信息
        let (all_free_blocks, written_multi_use_blocks) = {
            let mut state = self.block_state.lock();

            // all_free_blocks = free_list + modified_blocks + newly_used_blocks
            let mut all_free_blocks: BTreeSet<BlockId> = state.free_list.clone();

            // 处理 modified_blocks - 先收集到临时向量
            let modified_blocks_vec: Vec<BlockId> = state.modified_blocks.iter().copied().collect();
            for block_id in modified_blocks_vec {
                all_free_blocks.insert(block_id);
                if !self.block_is_registered(block_id) {
                    // 块不再被使用 → 完全空闲
                    state.free_list.insert(block_id);
                } else {
                    // 块仍在使用 → 加入 free_blocks_in_use
                    state.free_blocks_in_use.insert(block_id);
                }
            }

            // 新分配的块也算作"空闲"（对于这次 checkpoint）
            let mut written_multi_use_blocks = state.multi_use_blocks.clone();
            for &block_id in &state.newly_used_blocks {
                all_free_blocks.insert(block_id);
                written_multi_use_blocks.remove(&block_id);
            }

            (all_free_blocks, written_multi_use_blocks)
        };

        // 4. 写入 free list 数据
        let free_list_pointer = if !free_list_blocks.is_empty() {
            let mut writer = FreeListBlockWriter::new(metadata_manager, free_list_blocks);
            let ptr = writer.get_meta_block_pointer();

            // 写入 free_list
            writer.write_u64(all_free_blocks.len() as u64);
            for &block_id in &all_free_blocks {
                writer.write_i64(block_id);
            }

            // 写入 multi_use_blocks
            writer.write_u64(written_multi_use_blocks.len() as u64);
            for (&block_id, &ref_count) in &written_multi_use_blocks {
                writer.write_i64(block_id);
                writer.write_u32(ref_count);
            }

            // 写入 metadata 块注册表
            metadata_manager.write(&mut writer);

            // Flush 当前块
            writer.flush();

            ptr.block_pointer as i64
        } else {
            INVALID_BLOCK
        };

        // 5. 刷盘
        metadata_manager.flush();

        // 6. 更新 header 并写入
        header.free_list = free_list_pointer;
        header.block_count = self.block_state.lock().max_block as u64;
        header.iteration = self.block_state.lock().iteration_count + 1;

        self.write_header(header)?;

        // 7. 清理状态
        let mut state = self.block_state.lock();
        state.modified_blocks.clear();
        state.newly_used_blocks.clear();

        Ok(())
    }
}

// ─── BlockManager 实现 ───────────────────────────────────────

impl BlockManager for SingleFileBlockManager {
    fn buffer_manager(&self) -> Arc<dyn BufferManager> {
        Arc::clone(&self.buffer_manager)
    }

    fn try_get_buffer_pool(&self) -> Option<Arc<BufferPool>> {
        Some(self.buffer_manager.get_buffer_pool())
    }

    fn get_block_alloc_size(&self) -> usize {
        self.block_alloc_size
    }

    fn get_block_header_size(&self) -> usize {
        self.block_header_size
    }

    // ─── 注册/注销 ────────────────────────────────────────────

    /// 对应 C++ RegisterBlock(block_id_t id)
    ///
    /// 幂等：若已注册且 Weak 仍有效，返回已有 Arc；
    /// 否则从 BufferManager 创建新 BlockHandle 并弱引用存入表。
    fn register_block(&self, block_id: BlockId) -> Arc<BlockHandle> {
        let mut map = self.block_map.lock();
        if let Some(weak) = map.blocks.get(&block_id) {
            if let Some(arc) = weak.upgrade() {
                return arc;
            }
        }
        // 创建新的未加载 BlockHandle
        use crate::storage::buffer::{BufferPoolReservation, MemoryTracker};
        let tracker = Arc::clone(&self.buffer_manager.get_buffer_pool()) as Arc<dyn MemoryTracker>;
        let reservation = BufferPoolReservation::new(MemoryTag::BaseTable, tracker);
        let block_manager = self
            .self_ref
            .get()
            .and_then(|weak| weak.upgrade())
            .expect("SingleFileBlockManager self reference not initialized");
        let handle = BlockHandle::new_unloaded(
            block_manager,
            block_id,
            MemoryTag::BaseTable,
            self.block_alloc_size,
            reservation,
        );
        map.blocks.insert(block_id, Arc::downgrade(&handle));
        return handle;
        let handle = BlockHandle::new_unloaded(
            // 需要 Arc<dyn BlockManager> self-reference — 通过 Weak 模拟
            // 骨架：此处简化，实际需要 Arc<Self> 注入
            panic_block_manager(), // placeholder — 见下方注释
            block_id,
            MemoryTag::BaseTable,
            self.block_alloc_size,
            reservation,
        );
        map.blocks.insert(block_id, Arc::downgrade(&handle));
        handle
    }

    fn unregister_block(&self, block_id: BlockId) {
        self.block_map.lock().blocks.remove(&block_id);
    }

    fn block_is_registered(&self, block_id: BlockId) -> bool {
        self.block_map
            .lock()
            .blocks
            .get(&block_id)
            .and_then(|w| w.upgrade())
            .is_some()
    }

    // ─── Block 创建/读/写 ─────────────────────────────────────

    fn create_block(
        &self,
        _block_id: BlockId,
        reusable: Option<Box<FileBuffer>>,
    ) -> Box<FileBuffer> {
        let payload_size = self.block_alloc_size - self.block_header_size;
        if let Some(mut fb) = reusable {
            if fb.alloc_size() != self.block_alloc_size {
                fb.resize(payload_size);
            }
            fb
        } else {
            Box::new(FileBuffer::new(
                FileBufferType::Block,
                self.block_alloc_size,
                self.block_header_size,
            ))
        }
    }

    fn read_block(&self, block: &mut FileBuffer, block_id: BlockId) {
        let offset = self.block_offset(block_id);
        let mut file = self
            .fs
            .open_file(&self.path, FileOpenFlags::READ)
            .expect("Failed to open db file for reading");
        file.read_at(block.raw_mut(), offset)
            .expect("Failed to read block");
    }

    fn write_block(&self, block: &FileBuffer, block_id: BlockId) {
        debug_assert!(!self.read_only);
        let offset = self.block_offset(block_id);

        // 计算 checksum（从 payload 开始）
        let checksum_val = checksum(block.payload());

        // 创建一个临时 buffer 来存储带 checksum 的数据
        let mut write_buf = block.raw().to_vec();
        write_buf[0..8].copy_from_slice(&checksum_val.to_le_bytes());

        let mut file = self
            .fs
            .open_file(&self.path, FileOpenFlags::READ_WRITE)
            .expect("Failed to open db file for writing");
        file.write_at(&write_buf, offset)
            .expect("Failed to write block");
    }

    // ─── 转换 ─────────────────────────────────────────────────

    /// 对应 C++ ConvertBlock(block_id_t, FileBuffer &source)
    ///
    /// 将 source（通常是 ManagedBuffer）转换为 Block 格式的新 FileBuffer。
    fn convert_block(&self, _block_id: BlockId, source: &FileBuffer) -> Box<FileBuffer> {
        let mut fb = Box::new(FileBuffer::new(
            FileBufferType::Block,
            self.block_alloc_size,
            self.block_header_size,
        ));
        fb.copy_from(source.payload());
        fb
    }

    /// 对应 C++ ConvertToPersistent(QueryContext, block_id_t, Arc<BlockHandle>)
    fn convert_to_persistent(
        &self,
        block_id: BlockId,
        old_block: Arc<BlockHandle>,
    ) -> Arc<BlockHandle> {
        // 将内存临时块写入磁盘（分配一个真正的 block_id）
        {
            let guard = old_block.lock();
            if let Some(buf) = &guard.buffer {
                self.write_block(buf, block_id);
            }
        }
        // 注册为持久化块
        self.register_block(block_id)
    }

    // ─── 空闲块 ID 管理 ────────────────────────────────────────

    fn peek_free_block_id(&self) -> BlockId {
        let state = self.block_state.lock();
        if let Some(&id) = state.free_list.iter().next() {
            id
        } else {
            state.max_block
        }
    }

    fn get_free_block_id_for_checkpoint(&self) -> BlockId {
        self.get_free_block_id()
    }

    fn mark_block_as_modified(&self, block_id: BlockId) {
        self.block_state.lock().mark_as_modified(block_id);
    }

    fn block_count(&self) -> u64 {
        self.block_state.lock().max_block as u64
    }
}

// ─── 辅助：骨架阶段的占位 BlockManager ──────────────────────
//
// register_block() 需要向 BlockHandle::new_unloaded 传入 Arc<dyn BlockManager>
// 指向 self。但 self 此时没有 Arc（方法签名是 &self，不是 Arc<Self>）。
//
// 解决方案（后续迭代完善）：
//   在 SingleFileBlockManager 内存储 OnceLock<Weak<Self>>，
//   与 BlockHandle::weak_self 相同的模式，在 new() 返回 Arc<Self> 后初始化。
//
// 当前骨架：register_block 返回 BlockHandle 持有一个 panic 占位 BlockManager，
// 实际使用前需通过 set_self_weak() 正确初始化。

fn panic_block_manager() -> Arc<dyn BlockManager> {
    struct Placeholder;
    impl BlockManager for Placeholder {
        fn buffer_manager(&self) -> Arc<dyn BufferManager> {
            unimplemented!()
        }
        fn get_block_alloc_size(&self) -> usize {
            unimplemented!()
        }
        fn get_block_header_size(&self) -> usize {
            unimplemented!()
        }
        fn register_block(&self, _: BlockId) -> Arc<BlockHandle> {
            unimplemented!()
        }
        fn unregister_block(&self, _: BlockId) {}
        fn block_is_registered(&self, _: BlockId) -> bool {
            false
        }
        fn create_block(&self, _: BlockId, _: Option<Box<FileBuffer>>) -> Box<FileBuffer> {
            unimplemented!()
        }
        fn read_block(&self, _: &mut FileBuffer, _: BlockId) {
            unimplemented!()
        }
        fn write_block(&self, _: &FileBuffer, _: BlockId) {
            unimplemented!()
        }
        fn convert_block(&self, _: BlockId, _: &FileBuffer) -> Box<FileBuffer> {
            unimplemented!()
        }
        fn convert_to_persistent(&self, _: BlockId, _: Arc<BlockHandle>) -> Arc<BlockHandle> {
            unimplemented!()
        }
        fn metadata_manager(&self) -> Option<Arc<dyn Any + Send + Sync>> {
            None
        }
    }
    Arc::new(Placeholder)
}
