//! Catalog 顶层接口与 DuckCatalog 实现。
//!
//! 对应 C++:
//!   - `duckdb/catalog/catalog.hpp`（`class Catalog`）
//!   - `duckdb/catalog/duck_catalog.hpp`（`class DuckCatalog`）
//!
//! # 设计说明
//!
//! | C++ | Rust |
//! |-----|------|
//! | `class Catalog` (abstract) | `trait Catalog` |
//! | `class DuckCatalog : public Catalog` | `struct DuckCatalog` |
//! | `unique_ptr<CatalogSet> schemas` | `schemas: CatalogSet`（内部含 DefaultSchemaGenerator） |
//! | `unique_ptr<DependencyManager> dependency_manager` | `dependency_manager: DependencyManager` |
//! | `mutex write_lock` | `write_lock: Mutex<()>` |

use parking_lot::Mutex;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use super::catalog_set::{CatalogSet, LookupFailureReason};
use super::default_generator::DefaultSchemaGenerator;
use super::dependency::LogicalDependencyList;
use super::dependency_manager::DependencyManager;
use super::entry::{CatalogEntryBase, CatalogEntryKind, CatalogEntryNode};
use super::entry_lookup::{CatalogEntryLookup, EntryLookupInfo, SimilarCatalogEntry};
use super::error::CatalogError;
use super::catalog_entry::{DuckSchemaEntry, SchemaCatalogEntry};
use super::transaction::CatalogTransaction;
use super::types::{
    AlterInfo, CatalogLookupBehavior, CatalogType, CreateCollationInfo, CreateCopyFunctionInfo,
    CreateFunctionInfo, CreateIndexInfo, CreatePragmaFunctionInfo, CreateSchemaInfo,
    CreateSequenceInfo, CreateTableInfo, CreateTypeInfo, CreateViewInfo, DatabaseSize, DropInfo,
    MetadataBlockInfo, OnEntryNotFound, BoundCreateTableInfo,
};
use crate::common::errors::CatalogResult;
use crate::transaction::transaction::TransactionRef;

// ─── Catalog trait ─────────────────────────────────────────────────────────────

/// Catalog 抽象接口（C++: `class Catalog`）。
pub trait Catalog: Send + Sync {
    /// Catalog 名称（C++: `GetName`）。
    fn name(&self) -> &str;

    /// Catalog OID（C++: `GetOid`）。
    fn oid(&self) -> u64;

    /// Catalog 类型字符串（C++: `virtual string GetCatalogType() = 0`）。
    fn catalog_type_str(&self) -> &str;

    /// 是否为 DuckDB 原生 Catalog（C++: `virtual bool IsDuckCatalog()`）。
    fn is_duck_catalog(&self) -> bool {
        false
    }

    /// 是否为系统 Catalog（C++: `IsSystemCatalog`）。
    fn is_system_catalog(&self) -> bool {
        self.name() == "system" || self.name() == "temp"
    }

    /// 是否为临时 Catalog。
    fn is_temporary_catalog(&self) -> bool {
        self.name() == "temp"
    }

    /// 初始化（C++: `virtual void Initialize(bool load_builtin) = 0`）。
    fn initialize(&mut self, load_builtin: bool) -> CatalogResult<()>;

    /// 获取 Catalog 版本号（C++: `virtual optional_idx GetCatalogVersion`）。
    fn catalog_version(&self) -> Option<u64> {
        None
    }

    // ── Schema 操作 ───────────────────────────────────────────────────────────

    /// 创建 schema（C++: `virtual CreateSchema = 0`）。
    fn create_schema(
        &self,
        txn: &CatalogTransaction,
        info: &CreateSchemaInfo,
    ) -> CatalogResult<()>;

    /// 查找 schema（C++: `virtual LookupSchema = 0`）。
    fn lookup_schema(
        &self,
        txn: &CatalogTransaction,
        lookup: &EntryLookupInfo,
        if_not_found: OnEntryNotFound,
    ) -> CatalogResult<Option<String>>;

    /// 获取 schema（抛出异常版）（C++: `GetSchema(context, name)`）。
    fn get_schema(
        &self,
        txn: &CatalogTransaction,
        schema_name: &str,
    ) -> CatalogResult<String> {
        let lookup = EntryLookupInfo::schema_lookup(schema_name);
        self.lookup_schema(txn, &lookup, OnEntryNotFound::ThrowException)?
            .ok_or_else(|| CatalogError::not_found(CatalogType::SchemaEntry, schema_name))
    }

    /// 遍历所有 schema（C++: `virtual ScanSchemas = 0`）。
    fn scan_schemas(&self, txn: &CatalogTransaction, f: &mut dyn FnMut(&str));

    /// 获取所有 schema 名称列表（C++: `GetSchemas`）。
    fn get_all_schemas(&self, txn: &CatalogTransaction) -> Vec<String> {
        let mut schemas = Vec::new();
        self.scan_schemas(txn, &mut |name| schemas.push(name.to_string()));
        schemas
    }

    // ── 表/视图/函数等创建接口 ───────────────────────────────────────────────

    fn create_table(
        &self,
        txn: &CatalogTransaction,
        info: &BoundCreateTableInfo,
    ) -> CatalogResult<()>;
    fn create_view(
        &self,
        txn: &CatalogTransaction,
        info: &CreateViewInfo,
    ) -> CatalogResult<()>;
    fn create_sequence(
        &self,
        txn: &CatalogTransaction,
        info: &CreateSequenceInfo,
    ) -> CatalogResult<()>;
    fn create_type(
        &self,
        txn: &CatalogTransaction,
        info: &CreateTypeInfo,
    ) -> CatalogResult<()>;
    fn create_index(
        &self,
        txn: &CatalogTransaction,
        info: &CreateIndexInfo,
    ) -> CatalogResult<()>;
    fn create_function(
        &self,
        txn: &CatalogTransaction,
        info: &CreateFunctionInfo,
    ) -> CatalogResult<()>;
    fn create_table_function(
        &self,
        txn: &CatalogTransaction,
        info: &CreateFunctionInfo,
    ) -> CatalogResult<()>;
    fn create_copy_function(
        &self,
        txn: &CatalogTransaction,
        info: &CreateCopyFunctionInfo,
    ) -> CatalogResult<()>;
    fn create_pragma_function(
        &self,
        txn: &CatalogTransaction,
        info: &CreatePragmaFunctionInfo,
    ) -> CatalogResult<()>;
    fn create_collation(
        &self,
        txn: &CatalogTransaction,
        info: &CreateCollationInfo,
    ) -> CatalogResult<()>;

    // ── 条目查找 ──────────────────────────────────────────────────────────────

    fn get_entry(
        &self,
        txn: &CatalogTransaction,
        schema_name: &str,
        lookup: &EntryLookupInfo,
        if_not_found: OnEntryNotFound,
    ) -> CatalogResult<Option<CatalogEntryNode>>;

    // ── 修改与删除 ────────────────────────────────────────────────────────────

    fn drop_entry(&self, txn: &CatalogTransaction, info: &DropInfo) -> CatalogResult<()>;
    fn alter(&self, txn: &CatalogTransaction, info: &AlterInfo) -> CatalogResult<()>;

    // ── 统计信息 ──────────────────────────────────────────────────────────────

    fn database_size(&self, txn: &CatalogTransaction) -> DatabaseSize;
    fn metadata_info(&self, txn: &CatalogTransaction) -> Vec<MetadataBlockInfo>;
    fn in_memory(&self) -> bool;
    fn db_path(&self) -> String;

    fn supports_time_travel(&self) -> bool {
        false
    }
    fn is_encrypted(&self) -> bool {
        false
    }
    fn encryption_cipher(&self) -> String {
        String::new()
    }

    /// 查找行为（C++: `CatalogTypeLookupRule`）。
    fn catalog_type_lookup_rule(&self, _catalog_type: CatalogType) -> CatalogLookupBehavior {
        CatalogLookupBehavior::Standard
    }

    /// 默认 schema 名称（C++: `GetDefaultSchema`）。
    fn default_schema(&self) -> &str {
        "main"
    }
}

// ─── DuckCatalog ──────────────────────────────────────────────────────────────

/// DuckDB 原生 Catalog（C++: `class DuckCatalog`）。
pub struct DuckCatalog {
    /// Catalog 名称（等于所属 AttachedDatabase 的名称）。
    name: String,
    /// Catalog OID。
    oid: u64,
    /// Catalog 版本号（每次 schema 变更后递增）（C++: `catalog_version`）。
    catalog_version: AtomicU64,
    /// Schema 集合（C++: `unique_ptr<CatalogSet> schemas`）。
    pub schemas: CatalogSet,
    /// 依赖管理器（C++: `unique_ptr<DependencyManager> dependency_manager`）。
    pub dependency_manager: DependencyManager,
    /// 写锁（C++: `mutex write_lock`）。
    write_lock: Mutex<()>,
    /// 是否为内存数据库（C++: `InMemory`）。
    is_in_memory: bool,
    /// 数据库文件路径（空字符串表示内存数据库）。
    db_path_str: String,
    /// 是否已加密。
    encrypted: bool,
    /// 加密密钥 ID。
    encryption_key_id: String,
}

impl DuckCatalog {
    /// 创建内存数据库 Catalog。
    pub fn new_in_memory(name: impl Into<String>, oid: u64) -> Self {
        let name = name.into();
        Self::new_impl(name, oid, true, String::new())
    }

    /// 创建文件数据库 Catalog。
    pub fn new_file(name: impl Into<String>, oid: u64, path: impl Into<String>) -> Self {
        let name = name.into();
        Self::new_impl(name, oid, false, path.into())
    }

    fn new_impl(name: String, oid: u64, is_in_memory: bool, path: String) -> Self {
        // 创建带默认 schema 生成器的 schemas CatalogSet
        let schema_gen = Box::new(DefaultSchemaGenerator::new(oid, name.clone()));
        let schemas = CatalogSet::with_defaults(oid, schema_gen);

        Self {
            name: name.clone(),
            oid,
            catalog_version: AtomicU64::new(0),
            schemas,
            dependency_manager: DependencyManager::new(oid),
            write_lock: Mutex::new(()),
            is_in_memory,
            db_path_str: path,
            encrypted: false,
            encryption_key_id: String::new(),
        }
    }

    /// 获取写锁（调用方负责持有锁期间完成操作）（C++: `GetWriteLock`）。
    pub fn lock_write(&self) -> parking_lot::MutexGuard<'_, ()> {
        self.write_lock.lock()
    }

    /// 设置加密（C++: `SetIsEncrypted` / `SetEncryptionKeyId`）。
    pub fn set_encrypted(&mut self, key_id: impl Into<String>) {
        self.encrypted = true;
        self.encryption_key_id = key_id.into();
    }

    /// 获取 schema CatalogSet（C++: `GetSchemaCatalogSet`）。
    pub fn get_schema_catalog_set(&self) -> &CatalogSet {
        &self.schemas
    }

    /// 递增 catalog 版本号。
    pub fn increment_catalog_version(&self) -> u64 {
        self.catalog_version.fetch_add(1, Ordering::SeqCst) + 1
    }

    /// 内部 get_schema_for_operation，获取 DuckSchemaEntry 以执行操作。
    pub fn get_schema_for_op(
        &self,
        txn: &CatalogTransaction,
        schema_name: &str,
    ) -> CatalogResult<Arc<DuckSchemaEntry>> {
        self.schemas
            .get_entry(txn, schema_name)
            .ok_or_else(|| CatalogError::not_found(CatalogType::SchemaEntry, schema_name))
            .and_then(|entry| match entry.kind {
                CatalogEntryKind::Schema(schema) => Ok(schema),
                _ => Err(CatalogError::other(format!(
                    "Entry \"{}\" is not a schema entry",
                    schema_name
                ))),
            })
    }

    pub fn get_schema_for_operation(
        &self,
        txn: &CatalogTransaction,
        schema_name: &str,
    ) -> CatalogResult<Arc<DuckSchemaEntry>> {
        self.get_schema_for_op(txn, schema_name)
    }

    fn push_catalog_entry_ptr(
        &self,
        txn: &CatalogTransaction,
        catalog_entry_ptr: u64,
    ) -> CatalogResult<()> {
        let Some(transaction) = &txn.transaction else {
            return Ok(());
        };
        let Some(transaction_manager) = &txn.transaction_manager else {
            return Ok(());
        };
        let transaction_ref: TransactionRef = transaction.clone();
        transaction_manager
            .push_catalog_entry(
                &transaction_ref,
                crate::transaction::duck_transaction_manager::DbContext {
                    is_system: self.is_system_catalog(),
                    is_temporary: self.is_temporary_catalog(),
                    is_read_only: false,
                    storage_loaded: true,
                    storage_in_memory: self.is_in_memory,
                    compression_enabled: false,
                    debug_skip_checkpoint_on_commit: false,
                    has_wal: !self.is_in_memory,
                    recovery_mode_default: true,
                    storage_manager: None,
                },
                catalog_entry_ptr,
                &[],
            )
            .map_err(|e| CatalogError::other(e.to_string()))
    }

}

impl Catalog for DuckCatalog {
    fn name(&self) -> &str {
        &self.name
    }
    fn oid(&self) -> u64 {
        self.oid
    }
    fn catalog_type_str(&self) -> &str {
        "duckdb"
    }
    fn is_duck_catalog(&self) -> bool {
        true
    }
    fn catalog_version(&self) -> Option<u64> {
        Some(self.catalog_version.load(Ordering::SeqCst))
    }

    fn initialize(&mut self, load_builtin: bool) -> CatalogResult<()> {
        let sys_txn = CatalogTransaction::system(self.oid);

        // DuckDB: main 由 Initialize 显式创建，default generator 负责其余系统 schema。
        let mut main_info = CreateSchemaInfo::new("main".to_string());
        main_info.base.internal = true;
        main_info.base.on_conflict = super::types::OnCreateConflict::IgnoreOnConflict;
        let _ = self.create_schema(&sys_txn, &main_info);

        for schema_name in DefaultSchemaGenerator::DEFAULT_SCHEMAS {
            let mut info = CreateSchemaInfo::new(schema_name.to_string());
            info.base.internal = true;
            info.base.on_conflict = super::types::OnCreateConflict::IgnoreOnConflict;
            let _ = self.create_schema(&sys_txn, &info);
        }

        if load_builtin {
            // 在实际实现中，这里会加载内置函数、内置类型等
            // 此处仅做 schema 初始化
        }

        self.increment_catalog_version();
        Ok(())
    }

    // ── Schema 操作 ───────────────────────────────────────────────────────────

    fn create_schema(
        &self,
        txn: &CatalogTransaction,
        info: &CreateSchemaInfo,
    ) -> CatalogResult<()> {
        let _lock = self.write_lock.lock();
        let schema_name = info.schema_name();
        if !info.base.internal && DefaultSchemaGenerator::is_default_schema(schema_name) {
            return match info.base.on_conflict {
                super::types::OnCreateConflict::IgnoreOnConflict => Ok(()),
                _ => Err(CatalogError::already_exists(CatalogType::SchemaEntry, schema_name)),
            };
        }
        let oid = schema_name.len() as u64 ^ self.oid;
        let mut base = CatalogEntryBase::new(
            oid,
            CatalogType::SchemaEntry,
            schema_name.to_string(),
            self.name.clone(),
            String::new(),
        );
        base.temporary = info.base.temporary;
        base.internal = info.base.internal;
        base.comment = info.base.comment.clone();
        let schema = Arc::new(DuckSchemaEntry::new(
            oid,
            schema_name.to_string(),
            self.name.clone(),
            self.oid,
            info.base.internal,
        ));
        let node = Box::new(CatalogEntryNode::new(base, CatalogEntryKind::Schema(schema)));

        match self
            .schemas
            .create_entry(txn, schema_name, node, &LogicalDependencyList::new())
        {
            Ok(_) => {
                if let Some(entry_ptr) = self.schemas.get_undo_entry_ptr(schema_name) {
                    self.push_catalog_entry_ptr(txn, entry_ptr)?;
                }
                self.increment_catalog_version();
                Ok(())
            }
            Err(CatalogError::AlreadyExists { .. })
                if info.base.on_conflict == super::types::OnCreateConflict::IgnoreOnConflict =>
            {
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    fn lookup_schema(
        &self,
        txn: &CatalogTransaction,
        lookup: &EntryLookupInfo,
        if_not_found: OnEntryNotFound,
    ) -> CatalogResult<Option<String>> {
        let result = self.schemas.get_entry_detailed(txn, &lookup.name);
        match result.reason {
            LookupFailureReason::Success => Ok(Some(
                result
                    .node
                    .map(|n| n.base.name)
                    .unwrap_or_else(|| lookup.name.clone()),
            )),
            _ => match if_not_found {
                OnEntryNotFound::ReturnNull => Ok(None),
                OnEntryNotFound::ThrowException => {
                    let similar = self.schemas.similar_entry(txn, &lookup.name);
                    if similar.found() {
                        Err(CatalogError::not_found_with_hint(
                            CatalogType::SchemaEntry,
                            &lookup.name,
                            &similar.name,
                        ))
                    } else {
                        Err(CatalogError::not_found(
                            CatalogType::SchemaEntry,
                            &lookup.name,
                        ))
                    }
                }
            },
        }
    }

    fn scan_schemas(&self, txn: &CatalogTransaction, f: &mut dyn FnMut(&str)) {
        self.schemas.scan_with_txn(txn, |node| {
            if !node.base.deleted {
                f(&node.base.name);
            }
        });
    }

    // ── 表/视图等创建 ─────────────────────────────────────────────────────────

    fn create_table(
        &self,
        txn: &CatalogTransaction,
        info: &BoundCreateTableInfo,
    ) -> CatalogResult<()> {
        let _lock = self.write_lock.lock();
        let schema = self.get_schema_for_op(txn, &info.base.base.schema)?;
        schema.create_table(txn, info)?;
        if let Some(entry_ptr) = schema.tables.get_undo_entry_ptr(&info.base.table) {
            self.push_catalog_entry_ptr(txn, entry_ptr)?;
        }
        self.increment_catalog_version();
        Ok(())
    }

    fn create_view(
        &self,
        txn: &CatalogTransaction,
        info: &CreateViewInfo,
    ) -> CatalogResult<()> {
        let _lock = self.write_lock.lock();
        let schema = self.get_schema_for_op(txn, &info.base.schema)?;
        schema.create_view(txn, info)?;
        self.increment_catalog_version();
        Ok(())
    }

    fn create_sequence(
        &self,
        txn: &CatalogTransaction,
        info: &CreateSequenceInfo,
    ) -> CatalogResult<()> {
        let _lock = self.write_lock.lock();
        let schema = self.get_schema_for_op(txn, &info.base.schema)?;
        schema.create_sequence(txn, info)?;
        self.increment_catalog_version();
        Ok(())
    }

    fn create_type(
        &self,
        txn: &CatalogTransaction,
        info: &CreateTypeInfo,
    ) -> CatalogResult<()> {
        let _lock = self.write_lock.lock();
        let schema = self.get_schema_for_op(txn, &info.base.schema)?;
        schema.create_type(txn, info)?;
        self.increment_catalog_version();
        Ok(())
    }

    fn create_index(
        &self,
        txn: &CatalogTransaction,
        info: &CreateIndexInfo,
    ) -> CatalogResult<()> {
        let _lock = self.write_lock.lock();
        let schema = self.get_schema_for_op(txn, &info.base.schema)?;
        schema.create_index(txn, info)?;
        self.increment_catalog_version();
        Ok(())
    }

    fn create_function(
        &self,
        txn: &CatalogTransaction,
        info: &CreateFunctionInfo,
    ) -> CatalogResult<()> {
        let _lock = self.write_lock.lock();
        let schema = self.get_schema_for_op(txn, &info.base.schema)?;
        schema.create_function(txn, info)?;
        self.increment_catalog_version();
        Ok(())
    }

    fn create_table_function(
        &self,
        txn: &CatalogTransaction,
        info: &CreateFunctionInfo,
    ) -> CatalogResult<()> {
        self.create_function(txn, info)
    }

    fn create_copy_function(
        &self,
        txn: &CatalogTransaction,
        info: &CreateCopyFunctionInfo,
    ) -> CatalogResult<()> {
        self.create_function(txn, info)
    }

    fn create_pragma_function(
        &self,
        txn: &CatalogTransaction,
        info: &CreatePragmaFunctionInfo,
    ) -> CatalogResult<()> {
        self.create_function(txn, info)
    }

    fn create_collation(
        &self,
        txn: &CatalogTransaction,
        info: &CreateCollationInfo,
    ) -> CatalogResult<()> {
        let _lock = self.write_lock.lock();
        let schema = self.get_schema_for_op(txn, &info.base.schema)?;
        schema.create_collation(txn, info)?;
        self.increment_catalog_version();
        Ok(())
    }

    // ── 条目查找 ──────────────────────────────────────────────────────────────

    fn get_entry(
        &self,
        txn: &CatalogTransaction,
        schema_name: &str,
        lookup: &EntryLookupInfo,
        if_not_found: OnEntryNotFound,
    ) -> CatalogResult<Option<CatalogEntryNode>> {
        let schema = match self.get_schema_for_op(txn, schema_name) {
            Ok(s) => s,
            Err(e) => {
                return match if_not_found {
                    OnEntryNotFound::ReturnNull => Ok(None),
                    OnEntryNotFound::ThrowException => Err(e),
                };
            }
        };

        let entry = schema.lookup_entry(txn, lookup);
        match entry {
            Some(e) => Ok(Some(e)),
            None => match if_not_found {
                OnEntryNotFound::ReturnNull => Ok(None),
                OnEntryNotFound::ThrowException => {
                    let similar = schema.get_similar_entry(txn, lookup);
                    if similar.found() {
                        Err(CatalogError::not_found_with_hint(
                            lookup.catalog_type,
                            &lookup.name,
                            &similar.name,
                        ))
                    } else {
                        Err(CatalogError::not_found(lookup.catalog_type, &lookup.name))
                    }
                }
            },
        }
    }

    // ── 修改与删除 ────────────────────────────────────────────────────────────

    fn drop_entry(&self, txn: &CatalogTransaction, info: &DropInfo) -> CatalogResult<()> {
        let _lock = self.write_lock.lock();

        if info.catalog_type == CatalogType::SchemaEntry {
            // Drop schema：需要先 drop schema 内的所有对象
            if let Err(err) = self.get_schema_for_op(txn, &info.name) {
                if info.if_exists {
                    return Ok(());
                }
                return Err(err);
            }

            // 依赖检查
            let schema_info =
                super::dependency::CatalogEntryInfo::new(CatalogType::SchemaEntry, "", &info.name);
            let _to_drop = self
                .dependency_manager
                .check_drop(txn, &schema_info, info.cascade)?;

            self.schemas
                .drop_entry(txn, &info.name, info.cascade, info.allow_drop_internal)?;
            if let Some(entry_ptr) = self.schemas.get_undo_entry_ptr(&info.name) {
                self.push_catalog_entry_ptr(txn, entry_ptr)?;
            }
            self.increment_catalog_version();
            return Ok(());
        }

        // Drop schema 内的条目
        let schema = self.get_schema_for_op(txn, &info.schema)?;

        // 依赖检查
        let entry_info =
            super::dependency::CatalogEntryInfo::new(info.catalog_type, &info.schema, &info.name);
        let _to_drop = self
            .dependency_manager
            .check_drop(txn, &entry_info, info.cascade)?;
        // 真正的级联删除在实际实现中会递归删除 to_drop 中的条目

        schema.drop_entry(txn, info)?;
        if let Some(entry_ptr) = schema
            .get_catalog_set(info.catalog_type)
            .get_undo_entry_ptr(&info.name)
        {
            self.push_catalog_entry_ptr(txn, entry_ptr)?;
        }
        self.dependency_manager.erase_entry(txn, &entry_info)?;
        self.increment_catalog_version();
        Ok(())
    }

    fn alter(&self, txn: &CatalogTransaction, info: &AlterInfo) -> CatalogResult<()> {
        let _lock = self.write_lock.lock();
        let schema = self.get_schema_for_op(txn, &info.schema)?;
        schema.alter(txn, info)?;
        self.increment_catalog_version();
        Ok(())
    }

    // ── 统计信息 ──────────────────────────────────────────────────────────────

    fn database_size(&self, _txn: &CatalogTransaction) -> DatabaseSize {
        DatabaseSize::default()
    }

    fn metadata_info(&self, _txn: &CatalogTransaction) -> Vec<MetadataBlockInfo> {
        Vec::new()
    }

    fn in_memory(&self) -> bool {
        self.is_in_memory
    }

    fn db_path(&self) -> String {
        self.db_path_str.clone()
    }

    fn is_encrypted(&self) -> bool {
        self.encrypted
    }

    fn encryption_cipher(&self) -> String {
        if self.encrypted {
            "AES-256-GCM".to_string()
        } else {
            String::new()
        }
    }
}
