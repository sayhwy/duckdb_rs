// ============================================================
// storage/metadata/mod.rs
// 对应 C++: src/storage/metadata/ 目录下的所有文件
// ============================================================
//
// 模块依赖关系：
//   types             ← buffer（BlockId, BufferHandle）
//   metadata_block    ← types（ReadStream/WriteStream）
//   metadata_manager  ← metadata_block, types, buffer（BlockManager/BufferManager）
//   metadata_reader   ← metadata_manager, types
//   metadata_writer   ← metadata_manager, types

mod types;
mod metadata_block;
mod metadata_manager;
mod metadata_reader;
mod metadata_writer;
pub mod free_list_block_writer;

// ─── 公开导出 ─────────────────────────────────────────────────

pub use types::{
    MetaBlockPointer,
    BlockPointer,
    MetadataPointer,
    MetadataHandle,
    MetadataBlockInfo,
    ReadStream,
    WriteStream,
};

pub use metadata_block::MetadataBlock;

pub use metadata_manager::{
    MetadataManager,
    MetadataManagerInner,
    METADATA_BLOCK_COUNT,
};

pub use metadata_reader::{BlockReaderType, MetadataReader};
pub use metadata_writer::MetadataWriter;
