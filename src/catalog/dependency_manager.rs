//! 依赖关系管理器。
//!
//! 对应 C++:
//!   - `duckdb/catalog/dependency_manager.hpp`（`DependencyManager`）
//!   - `duckdb/catalog/dependency_catalog_set.hpp`（`DependencyCatalogSet`）
//!
//! # 设计说明
//!
//! DependencyManager 维护两个 CatalogSet：
//! - `subjects`：被依赖方视图（key = mangled(subject) → DependencySubjectEntry）
//! - `dependents`：依赖方视图（key = mangled(dependent) → DependencyDependentEntry）
//!
//! 每条依赖 (A depends on B) 会在两个 set 中各存一条记录：
//! - subjects[mangled(B)][mangled(A)] → DependencyDependentFlags（A 依赖 B 的方式）
//! - dependents[mangled(A)][mangled(B)] → DependencySubjectFlags（B 被 A 依赖的方式）
//!
//! | C++ | Rust |
//! |-----|------|
//! | `CatalogSet subjects` | `subjects: CatalogSet` |
//! | `CatalogSet dependents` | `dependents: CatalogSet` |
//! | `DependencyCatalogSet` | `DependencyCatalogSet` |

use std::collections::HashMap;
use parking_lot::Mutex;

use super::types::CatalogType;
use super::error::CatalogError;
use super::dependency::{
    CatalogEntryInfo, DependencyInfo, DependencySubject, DependencyDependent,
    DependencySubjectFlags, DependencyDependentFlags,
    MangledEntryName, MangledDependencyName, LogicalDependencyList,
};
use super::transaction::CatalogTransaction;
use super::entry::{CatalogEntryBase, CatalogEntryKind, CatalogEntryNode, DependencyRelationData};
use super::catalog_set::{CatalogSet, LookupFailureReason};

// ─── DependencyCatalogSet ──────────────────────────────────────────────────────

/// 为单个 catalog 条目维护的依赖子集（C++: `DependencyCatalogSet`）。
///
/// 包装 CatalogSet，键为 MangledDependencyName。
pub struct DependencyCatalogSet<'a> {
    set: &'a CatalogSet,
    _info: CatalogEntryInfo,
    _mangled_name: MangledEntryName,
}

impl<'a> DependencyCatalogSet<'a> {
    pub fn new(set: &'a CatalogSet, info: CatalogEntryInfo) -> Self {
        let mangled_name = MangledEntryName::new(&info);
        Self { set, _info: info, _mangled_name: mangled_name }
    }

    /// 创建依赖记录（C++: `DependencyCatalogSet::CreateEntry`）。
    pub fn create_entry(
        &self,
        txn: &CatalogTransaction,
        dep_name: &MangledEntryName,
        data: DependencyRelationData,
    ) -> Result<bool, CatalogError> {
        let oid = dep_name.name.len() as u64;
        let mut base = CatalogEntryBase::new(
            oid,
            CatalogType::DependencyEntry,
            dep_name.name.clone(),
            String::new(),
            String::new(),
        );
        base.internal = true;
        base.set_timestamp(txn.transaction_id);
        let node = Box::new(CatalogEntryNode::new(base, CatalogEntryKind::DependencyRelation(data)));
        self.set.create_entry(txn, &dep_name.name, node, &LogicalDependencyList::new())
    }

    /// 查找依赖记录（C++: `DependencyCatalogSet::GetEntry`）。
    pub fn get_entry(
        &self,
        txn: &CatalogTransaction,
        dep_name: &MangledEntryName,
    ) -> Option<CatalogEntryNode> {
        self.set.get_entry(txn, &dep_name.name)
    }

    /// 删除依赖记录（C++: `DependencyCatalogSet::DropEntry`）。
    pub fn drop_entry(
        &self,
        txn: &CatalogTransaction,
        dep_name: &MangledEntryName,
    ) -> Result<(), CatalogError> {
        self.set.drop_entry(txn, &dep_name.name, false, true)
    }

    /// 遍历所有依赖记录（C++: `DependencyCatalogSet::Scan`）。
    pub fn scan<F>(&self, txn: &CatalogTransaction, mut f: F)
    where
        F: FnMut(&CatalogEntryNode),
    {
        self.set.scan_with_txn(txn, |node| f(node));
    }
}

// ─── DependencyManager ────────────────────────────────────────────────────────

/// Catalog 依赖关系管理器（C++: `class DependencyManager`）。
///
/// 追踪 catalog 条目之间的依赖关系，在 DROP 时进行级联或阻止删除。
pub struct DependencyManager {
    /// 被依赖方视图（key = mangled(subject)）（C++: `CatalogSet subjects`）。
    pub subjects:  CatalogSet,
    /// 依赖方视图（key = mangled(dependent)）（C++: `CatalogSet dependents`）。
    pub dependents: CatalogSet,
    /// 所属 catalog 的 OID。
    catalog_oid: u64,
    /// 写锁（确保依赖操作的原子性）。
    write_lock: Mutex<()>,
}

impl DependencyManager {
    pub fn new(catalog_oid: u64) -> Self {
        Self {
            subjects:   CatalogSet::new(catalog_oid),
            dependents: CatalogSet::new(catalog_oid),
            catalog_oid,
            write_lock: Mutex::new(()),
        }
    }

    // ─── Mangle 辅助 ─────────────────────────────────────────────────────────

    /// 获取条目的 MangledEntryName（C++: `DependencyManager::MangleName(CatalogEntry&)`）。
    pub fn mangle_entry(info: &CatalogEntryInfo) -> MangledEntryName {
        MangledEntryName::new(info)
    }

    /// 获取 schema 名称（C++: `DependencyManager::GetSchema`）。
    pub fn get_schema(entry_type: CatalogType) -> &'static str {
        match entry_type {
            CatalogType::SchemaEntry => "",
            _ => "main",
        }
    }

    // ─── 依赖操作 ─────────────────────────────────────────────────────────────

    /// 添加一组依赖（C++: `DependencyManager` 内部方法，在 CreateEntry 时调用）。
    ///
    /// 对于 `dep_list` 中的每个依赖 (subject)：
    /// - subjects[subject] += dependent（DependencyDependentFlags::BLOCKING）
    /// - dependents[dependent] += subject（DependencySubjectFlags 默认）
    pub fn add_dependencies(
        &self,
        txn: &CatalogTransaction,
        dependent_info: &CatalogEntryInfo,
        dep_list: &LogicalDependencyList,
    ) -> Result<(), CatalogError> {
        let _lock = self.write_lock.lock();
        let dep_mangled = MangledEntryName::new(dependent_info);

        for logical_dep in dep_list.iter() {
            let subject_info = &logical_dep.entry;
            let subject_mangled = MangledEntryName::new(subject_info);
            let dep_dep_name = MangledDependencyName::new(&subject_mangled, &dep_mangled);

            // subjects[subject_mangled] 中添加 dependent 记录
            let subj_data = DependencyRelationData {
                entry_info: dependent_info.clone(),
                subject_flags: DependencySubjectFlags::new(),
                dependent_flags: {
                    let mut f = DependencyDependentFlags::new();
                    f.set_blocking();
                    f
                },
            };
            let subj_dep_set = DependencyCatalogSet::new(&self.subjects, subject_info.clone());
            let _ = subj_dep_set.create_entry(
                txn,
                &MangledEntryName { name: dep_dep_name.name.clone() },
                subj_data,
            );

            // dependents[dependent_mangled] 中添加 subject 记录
            let dep_subj_name = MangledDependencyName::new(&dep_mangled, &subject_mangled);
            let dep_data = DependencyRelationData {
                entry_info: subject_info.clone(),
                subject_flags: DependencySubjectFlags::new(),
                dependent_flags: DependencyDependentFlags::new(),
            };
            let dep_dep_set = DependencyCatalogSet::new(&self.dependents, dependent_info.clone());
            let _ = dep_dep_set.create_entry(
                txn,
                &MangledEntryName { name: dep_subj_name.name.clone() },
                dep_data,
            );
        }
        Ok(())
    }

    /// 添加所有权关系（C++: `DependencyManager::AddOwnership`）。
    ///
    /// owner 拥有 owned_by：当 owner 被删除时，owned_by 也会被级联删除。
    pub fn add_ownership(
        &self,
        txn: &CatalogTransaction,
        owner_info: &CatalogEntryInfo,
        owned_info: &CatalogEntryInfo,
    ) -> Result<(), CatalogError> {
        let _lock = self.write_lock.lock();
        let owner_mangled = MangledEntryName::new(owner_info);
        let owned_mangled = MangledEntryName::new(owned_info);

        // subjects[owned] += owner (OWNERSHIP flag)
        let mut subj_flags = DependencySubjectFlags::new();
        subj_flags.set_ownership();
        let mut dep_flags = DependencyDependentFlags::new();
        dep_flags.set_owned_by();

        let ownership_name = MangledDependencyName::new(&owned_mangled, &owner_mangled);
        let subj_data = DependencyRelationData {
            entry_info: owner_info.clone(),
            subject_flags: subj_flags,
            dependent_flags: dep_flags,
        };
        let subj_set = DependencyCatalogSet::new(&self.subjects, owned_info.clone());
        let _ = subj_set.create_entry(
            txn,
            &MangledEntryName { name: ownership_name.name.clone() },
            subj_data,
        );

        Ok(())
    }

    /// 检查删除条目是否违反依赖关系（C++: `DependencyManager` Drop 检查）。
    ///
    /// 如果有其他条目依赖 subject 且非 cascade，则返回错误。
    pub fn check_drop(
        &self,
        txn: &CatalogTransaction,
        subject_info: &CatalogEntryInfo,
        cascade: bool,
    ) -> Result<Vec<CatalogEntryInfo>, CatalogError> {
        // 收集所有依赖 subject 的条目
        let mut to_drop = Vec::new();
        let subject_mangled = MangledEntryName::new(subject_info);
        let subj_dep_set = DependencyCatalogSet::new(&self.subjects, subject_info.clone());

        subj_dep_set.scan(txn, |node| {
            if let CatalogEntryKind::DependencyRelation(data) = &node.kind {
                if data.dependent_flags.is_blocking() && !cascade {
                    // 阻塞依赖：非 cascade 时不能删除
                    to_drop.push(data.entry_info.clone());
                } else if data.dependent_flags.is_owned_by() || cascade {
                    to_drop.push(data.entry_info.clone());
                }
            }
        });

        if !cascade && !to_drop.is_empty() {
            let names: Vec<_> = to_drop.iter().map(|i| format!("{}.{}", i.schema, i.name)).collect();
            return Err(CatalogError::dependency_violation(
                &subject_info.name,
                format!("used by: {}", names.join(", ")),
            ));
        }

        Ok(to_drop)
    }

    /// 删除某条目的所有依赖记录（C++: `DependencyManager` Drop 清理）。
    pub fn erase_entry(
        &self,
        txn: &CatalogTransaction,
        info: &CatalogEntryInfo,
    ) -> Result<(), CatalogError> {
        let _lock = self.write_lock.lock();
        let mangled = MangledEntryName::new(info);

        // 从 subjects 中删除 info 的所有 dependent 记录（scan + drop）
        // 从 dependents 中删除 info 的所有 subject 记录（scan + drop）

        // 删除 subjects[info] 下的所有记录
        let subj_set = DependencyCatalogSet::new(&self.subjects, info.clone());
        let mut keys_to_drop: Vec<String> = Vec::new();
        subj_set.scan(txn, |node| {
            keys_to_drop.push(node.base.name.clone());
        });
        for key in &keys_to_drop {
            let _ = self.subjects.drop_entry(txn, key, true, true);
        }

        // 删除 dependents[info] 下的所有记录
        let dep_set = DependencyCatalogSet::new(&self.dependents, info.clone());
        let mut dep_keys: Vec<String> = Vec::new();
        dep_set.scan(txn, |node| {
            dep_keys.push(node.base.name.clone());
        });
        for key in &dep_keys {
            let _ = self.dependents.drop_entry(txn, key, true, true);
        }

        Ok(())
    }

    /// 遍历所有依赖关系（C++: `DependencyManager::Scan`）。
    pub fn scan<F>(&self, txn: &CatalogTransaction, mut f: F)
    where
        F: FnMut(&CatalogEntryInfo, &CatalogEntryInfo, &DependencyDependentFlags),
    {
        self.subjects.scan_with_txn(txn, |node| {
            if let CatalogEntryKind::DependencyRelation(data) = &node.kind {
                // node.base.name 是 mangled dependency 名称；entry_info 是 dependent
                f(&data.entry_info, &data.entry_info, &data.dependent_flags);
            }
        });
    }

    /// 对需要删除的条目进行拓扑排序（C++: `DependencyManager::ReorderEntries`）。
    pub fn reorder_entries(&self, entries: &mut Vec<CatalogEntryInfo>) {
        // 简单实现：按类型排序（视图/索引先删，表后删，schema 最后）
        entries.sort_by_key(|e| match e.entry_type {
            CatalogType::IndexEntry        => 0u8,
            CatalogType::ViewEntry         => 1,
            CatalogType::TableEntry        => 2,
            CatalogType::SequenceEntry     => 3,
            CatalogType::TypeEntry         => 4,
            CatalogType::SchemaEntry       => 5,
            _                              => 3,
        });
    }
}
