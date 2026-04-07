use crate::catalog::TableCatalogEntry;
use crate::common::serializer::BinarySerializer;
use crate::storage::metadata::{MetaBlockPointer, MetadataManager, MetadataWriter, WriteStream};
use crate::storage::storage_manager::CheckpointOptions;
use crate::storage::table::data_table_info::DataTableInfo;
use crate::storage::table::row_group::RowGroupPointer;
use crate::storage::table::row_group_collection::RowGroupCollection;
use crate::storage::table::table_statistics::TableStatistics;

#[derive(Debug, Clone, Copy, Default)]
pub struct QueryContext {
    pub client_id: u64,
}

pub struct SingleFileCheckpointWriter<'mgr> {
    pub checkpoint_options: CheckpointOptions,
    pub metadata_manager: &'mgr MetadataManager,
    pub partial_block_manager_id: u64,
}

impl<'mgr> SingleFileCheckpointWriter<'mgr> {
    pub fn get_checkpoint_options(&self) -> CheckpointOptions {
        self.checkpoint_options.clone()
    }

    pub fn get_metadata_manager(&self) -> &'mgr MetadataManager {
        self.metadata_manager
    }
}

#[derive(Debug, Default)]
pub struct TableDataWriter {
    pub table_name: String,
    pub context: Option<QueryContext>,
    pub row_group_pointers: Vec<RowGroupPointer>,
    pub override_base_stats: bool,
}

impl TableDataWriter {
    pub fn new(table: &TableCatalogEntry, context: QueryContext) -> Self {
        Self {
            table_name: table.base.fields().name.clone(),
            context: Some(context),
            row_group_pointers: Vec::new(),
            override_base_stats: true,
        }
    }

    pub fn write_table_data(&mut self) {}

    pub fn add_row_group(&mut self, row_group_pointer: RowGroupPointer) {
        self.row_group_pointers.push(row_group_pointer);
    }

    pub fn can_override_base_stats(&self) -> bool {
        self.override_base_stats
    }

    pub fn set_cannot_override_stats(&mut self) {
        self.override_base_stats = false;
    }

    pub fn create_task_executor(&self) -> TaskExecutor {
        TaskExecutor {
            client_id: self.context.map(|ctx| ctx.client_id),
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct TaskExecutor {
    pub client_id: Option<u64>,
}

pub struct SingleFileTableDataWriter<'writer, 'mgr> {
    pub base: TableDataWriter,
    pub checkpoint_manager: &'writer SingleFileCheckpointWriter<'mgr>,
    pub table_data_writer: &'writer mut MetadataWriter<'mgr>,
    pub existing_pointer: Option<MetaBlockPointer>,
    pub existing_rows: Option<u64>,
    pub existing_pointers: Vec<MetaBlockPointer>,
}

impl<'writer, 'mgr> SingleFileTableDataWriter<'writer, 'mgr> {
    pub fn new(
        checkpoint_manager: &'writer SingleFileCheckpointWriter<'mgr>,
        table: &TableCatalogEntry,
        table_data_writer: &'writer mut MetadataWriter<'mgr>,
    ) -> Self {
        Self {
            base: TableDataWriter::new(table, QueryContext::default()),
            checkpoint_manager,
            table_data_writer,
            existing_pointer: None,
            existing_rows: None,
            existing_pointers: Vec::new(),
        }
    }

    pub fn get_checkpoint_options(&self) -> CheckpointOptions {
        self.checkpoint_manager.get_checkpoint_options()
    }

    pub fn get_metadata_manager(&self) -> &'mgr MetadataManager {
        self.checkpoint_manager.get_metadata_manager()
    }

    pub fn write_unchanged_table(&mut self, pointer: MetaBlockPointer, total_rows: u64) {
        self.existing_pointer = Some(pointer);
        self.existing_rows = Some(total_rows);
    }

    pub fn flush_partial_blocks(&mut self) {}

    pub fn finalize_table(
        &mut self,
        global_stats: &TableStatistics,
        _info: &DataTableInfo,
        _collection: &RowGroupCollection,
    ) -> (MetaBlockPointer, u64) {
        if let Some(pointer) = self.existing_pointer {
            return (pointer, self.existing_rows.unwrap_or(0));
        }

        let pointer = self.table_data_writer.get_meta_block_pointer();
        let total_rows = self
            .base
            .row_group_pointers
            .iter()
            .map(|rg| rg.row_start + rg.tuple_count)
            .max()
            .unwrap_or(0);

        {
            let mut serializer =
                BinarySerializer::new(self.table_data_writer as &mut dyn WriteStream);
            serializer.begin_root_object();
            global_stats.serialize_checkpoint(&mut serializer);
            serializer.end_object();
        }
        self.table_data_writer
            .write_u64(self.base.row_group_pointers.len() as u64);
        for row_group_pointer in &self.base.row_group_pointers {
            let mut serializer =
                BinarySerializer::new(self.table_data_writer as &mut dyn WriteStream);
            serializer.begin_root_object();
            serializer.write_varint(100, row_group_pointer.row_start);
            serializer.write_varint(101, row_group_pointer.tuple_count);
            serializer.begin_list(102, row_group_pointer.column_pointers.len());
            for column_pointer in &row_group_pointer.column_pointers {
                serializer.list_write_object(|s| {
                    if column_pointer.block_pointer != 0 {
                        s.write_varint(100, column_pointer.block_pointer);
                    }
                    if column_pointer.offset != 0 {
                        s.write_varint(101, column_pointer.offset as u64);
                    }
                });
            }
            serializer.end_list();
            serializer.begin_list(103, row_group_pointer.deletes_pointers.len());
            for delete_pointer in &row_group_pointer.deletes_pointers {
                serializer.list_write_object(|s| {
                    if delete_pointer.block_pointer != 0 {
                        s.write_varint(100, delete_pointer.block_pointer);
                    }
                    if delete_pointer.offset != 0 {
                        s.write_varint(101, delete_pointer.offset as u64);
                    }
                });
            }
            serializer.end_list();
            serializer.end_object();
        }
        (pointer, total_rows)
    }
}
