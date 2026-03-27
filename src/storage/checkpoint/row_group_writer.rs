use crate::catalog::TableCatalogEntry;
use crate::storage::metadata::{MetaBlockPointer, MetadataManager, MetadataWriter, WriteStream};
use crate::storage::storage_manager::CheckpointOptions;

#[derive(Debug, Clone, Default)]
pub struct RowGroupWriter {
    pub table_name: String,
    pub partial_block_manager_id: u64,
    pub compression_types: Vec<String>,
}

impl RowGroupWriter {
    pub fn new(table: &TableCatalogEntry, partial_block_manager_id: u64) -> Self {
        let compression_types = table
            .get_columns()
            .columns
            .iter()
            .map(|_| "UNCOMPRESSED".to_string())
            .collect();
        Self {
            table_name: table.base.fields().name.clone(),
            partial_block_manager_id,
            compression_types,
        }
    }

    pub fn get_compression_types(&self) -> &[String] {
        &self.compression_types
    }
}

pub struct SingleFileRowGroupWriter<'writer, 'mgr> {
    pub base: RowGroupWriter,
    checkpoint_options: CheckpointOptions,
    table_data_writer: &'writer mut MetadataWriter<'mgr>,
    metadata_manager: Option<&'mgr MetadataManager>,
}

impl<'writer, 'mgr> SingleFileRowGroupWriter<'writer, 'mgr> {
    pub fn new(
        table: &TableCatalogEntry,
        partial_block_manager_id: u64,
        checkpoint_options: CheckpointOptions,
        table_data_writer: &'writer mut MetadataWriter<'mgr>,
        metadata_manager: Option<&'mgr MetadataManager>,
    ) -> Self {
        Self {
            base: RowGroupWriter::new(table, partial_block_manager_id),
            checkpoint_options,
            table_data_writer,
            metadata_manager,
        }
    }

    pub fn get_checkpoint_options(&self) -> CheckpointOptions {
        self.checkpoint_options.clone()
    }

    pub fn get_payload_writer(&mut self) -> &mut dyn WriteStream {
        self.table_data_writer
    }

    pub fn get_meta_block_pointer(&mut self) -> MetaBlockPointer {
        self.table_data_writer.get_meta_block_pointer()
    }

    pub fn get_metadata_manager(&self) -> Option<&MetadataManager> {
        self.metadata_manager
    }

    pub fn start_writing_columns(&mut self, column_metadata: Vec<MetaBlockPointer>) {
        self.table_data_writer
            .set_written_pointers(Some(column_metadata));
    }

    pub fn finish_writing_columns(&mut self) {
        self.table_data_writer.set_written_pointers(None);
    }
}
