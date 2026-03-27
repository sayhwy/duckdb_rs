//! Catalog 条目集合（带 MVCC 版本链）。
//!
//! 对应 C++:
//!   - `duckdb/catalog/catalog_entry_map.hpp`（`CatalogEntryMap`）
//!   - `duckdb/catalog/catalog_set.hpp`（`CatalogSet`）
//!
//! # 设计说明
//!
//! | C++ | Rust |
//! |-----|------|
//! | `case_insensitive_tree_t<unique_ptr<CatalogEntry>>` | `BTreeMap<String, ChainHead>`（小写键） |
//! | `unique_ptr<CatalogEntry> child`（旧版本） | `older: Option<Box<CatalogEntryNode>>`  |
//! | `mutex catalog_lock` | `RwLock<CatalogEntryMapInner>` |
//! | `unique_ptr<DefaultGenerator> defaults` | `defaults: Option<Box<dyn DefaultGenerator>>` |
//!
//! ## MVCC 链操作
//!
//! ```text
//! CreateEntry: [new_node] -> [old_head] -> ...
//! GetEntry: 从链头向旧版本走，找第一个可见节点
//! Undo: 弹出链头，链头的 older 成为新链头
//! CommitDrop: 将最新墓碑节点的时间戳设为 commit_id
//! CleanupEntry: 清理旧版本（不再对任何活跃事务可见的节点）
//! ```

use std::collections::BTreeMap;
use parking_lot::RwLock;

use super::types::{CatalogType, CreateInfo, AlterInfo, DropInfo};
use super::error::CatalogError;
use super::dependency::LogicalDependencyList;
use super::transaction::{CatalogTransaction, is_visible, has_conflict};
use super::entry::{CatalogEntryBase, CatalogEntryKind, CatalogEntryNode};
use super::entry_lookup::SimilarCatalogEntry;
use super::default_generator::DefaultGenerator;
use crate::transaction::types::{TransactionId, TRANSACTION_ID_START};

// ─── EntryLookupResult ─────────────────────────────────────────────────────────

/// GetEntry 的详细结果（C++: `CatalogSet::EntryLookup`）。
#[derive(Debug)]
pub struct EntryLookupResult {
    pub node: Option<Box<CatalogEntryNode>>,
    pub reason: LookupFailureReason,
}

/// 查找失败的原因（C++: `EntryLookup::FailureReason`）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LookupFailureReason {
    Success,
    Deleted,
    NotPresent,
    Invisible,
}

// ─── CatalogEntryMapInner ──────────────────────────────────────────────────────

/// CatalogEntryMap 的内部数据（受 RwLock 保护）。
struct CatalogEntryMapInner {
    /// 以小写名称为键的 MVCC 链头映射。
    entries: BTreeMap<String, Box<CatalogEntryNode>>,
}

impl CatalogEntryMapInner {
    fn new() -> Self { Self { entries: BTreeMap::new() } }

    /// 获取指定名称的链头（不考虑 MVCC 可见性）。
    fn get_raw(&self, lower_name: &str) -> Option<&CatalogEntryNode> {
        self.entries.get(lower_name).map(|b| b.as_ref())
    }

    /// 获取对事务可见的条目（走完 MVCC 链）。
    fn get_visible<'a>(&'a self, txn: &CatalogTransaction, lower_name: &str) -> Option<&'a CatalogEntryNode> {
        let mut current = self.entries.get(lower_name)?.as_ref();
        loop {
            if is_visible(txn, current.base.get_timestamp()) {
                return Some(current);
            }
            current = current.older.as_deref()?;
        }
    }

    /// 详细查找（C++: `GetEntryDetailed`）。
    fn get_visible_detailed(&self, txn: &CatalogTransaction, lower_name: &str) -> (Option<&CatalogEntryNode>, LookupFailureReason) {
        let head = match self.entries.get(lower_name) {
            None => return (None, LookupFailureReason::NotPresent),
            Some(h) => h.as_ref(),
        };

        let mut current = head;
        loop {
            let ts = current.base.get_timestamp();
            if is_visible(txn, ts) {
                if current.base.deleted {
                    return (Some(current), LookupFailureReason::Deleted);
                }
                return (Some(current), LookupFailureReason::Success);
            }
            match current.older.as_deref() {
                Some(older) => current = older,
                None => return (None, LookupFailureReason::Invisible),
            }
        }
    }

    /// 将新节点推入链头（旧链头成为 older）。
    fn push_head(&mut self, lower_name: String, mut new_node: Box<CatalogEntryNode>) {
        let old_head = self.entries.remove(&lower_name);
        new_node.older = old_head;
        self.entries.insert(lower_name, new_node);
    }

    /// 弹出链头（Undo 操作）：将链头的 older 提升为新链头。
    fn pop_head(&mut self, lower_name: &str) -> Option<Box<CatalogEntryNode>> {
        let mut head = self.entries.remove(lower_name)?;
        let older = head.older.take();
        if let Some(older) = older {
            self.entries.insert(lower_name.to_string(), older);
        }
        Some(head)
    }
}

// ─── CatalogEntryMap ──────────────────────────────────────────────────────────

/// 大小写不敏感的 Catalog 条目映射（C++: `CatalogEntryMap`）。
pub struct CatalogEntryMap {
    inner: RwLock<CatalogEntryMapInner>,
}

impl CatalogEntryMap {
    pub fn new() -> Self {
        Self { inner: RwLock::new(CatalogEntryMapInner::new()) }
    }

    fn lower(name: &str) -> String { name.to_lowercase() }

    /// 添加新条目（条目名称已存在时会 panic；应先检查）。
    pub fn add_entry(&self, node: Box<CatalogEntryNode>) {
        let key = Self::lower(&node.base.name);
        self.inner.write().entries.insert(key, node);
    }

    /// 更新（替换）已有条目链头。
    pub fn update_entry(&self, node: Box<CatalogEntryNode>) {
        let key = Self::lower(&node.base.name);
        self.inner.write().push_head(key, node);
    }

    /// 直接取出条目链头（不含 older 链）。
    pub fn drop_entry_raw(&self, name: &str) {
        let key = Self::lower(name);
        self.inner.write().entries.remove(&key);
    }
}

// ─── CatalogSet ───────────────────────────────────────────────────────────────

/// 一类 Catalog 条目的集合，支持 MVCC（C++: `CatalogSet`）。
pub struct CatalogSet {
    /// 条目映射（受内部 RwLock 保护）。
    map: RwLock<CatalogEntryMapInner>,
    /// 默认条目生成器（C++: `unique_ptr<DefaultGenerator> defaults`）。
    pub defaults: Option<Box<dyn DefaultGenerator>>,
    /// 所属 Catalog 的 OID（用于错误上报）。
    pub catalog_oid: u64,
}

impl CatalogSet {
    pub fn new(catalog_oid: u64) -> Self {
        Self { map: RwLock::new(CatalogEntryMapInner::new()), defaults: None, catalog_oid }
    }

    pub fn with_defaults(catalog_oid: u64, defaults: Box<dyn DefaultGenerator>) -> Self {
        Self { map: RwLock::new(CatalogEntryMapInner::new()), defaults: Some(defaults), catalog_oid }
    }

    pub fn set_default_generator(&mut self, generator: Box<dyn DefaultGenerator>) {
        self.defaults = Some(generator);
    }

    // ─── 写操作（需要持有 catalog 级写锁，由调用方保证） ─────────────────────────

    /// 创建条目（C++: `CatalogSet::CreateEntry`）。
    ///
    /// 返回 `Ok(true)` 表示成功创建，`Ok(false)` 表示条目已存在（仅在 IgnoreOnConflict 时）。
    pub fn create_entry(
        &self,
        txn: &CatalogTransaction,
        name: &str,
        mut node: Box<CatalogEntryNode>,
        _dependencies: &LogicalDependencyList,
    ) -> Result<bool, CatalogError> {
        let lower = name.to_lowercase();
        let mut map = self.map.write();

        // 检查现有条目
        let (existing, reason) = map.get_visible_detailed(txn, &lower);
        match reason {
            LookupFailureReason::Success => {
                // 条目已存在（活跃且可见）
                if let Some(existing) = existing {
                    // 检查写-写冲突
                    let ts = existing.base.get_timestamp();
                    if has_conflict(txn, ts) {
                        return Err(CatalogError::transaction_conflict(name));
                    }
                }
                return Err(CatalogError::already_exists(node.base.entry_type, name));
            }
            LookupFailureReason::Invisible => {
                // 其他活跃事务创建的条目，冲突
                if let Some(existing) = existing {
                    if has_conflict(txn, existing.base.get_timestamp()) {
                        return Err(CatalogError::transaction_conflict(name));
                    }
                }
            }
            LookupFailureReason::Deleted | LookupFailureReason::NotPresent => {
                // 可以创建
            }
        }

        // 设置时间戳为当前事务 ID
        node.base.set_timestamp(txn.transaction_id);
        map.push_head(lower, node);
        Ok(true)
    }

    /// 创建或替换条目（ON CONFLICT REPLACE 语义）。
    pub fn create_or_replace_entry(
        &self,
        txn: &CatalogTransaction,
        name: &str,
        mut node: Box<CatalogEntryNode>,
        dependencies: &LogicalDependencyList,
    ) -> Result<bool, CatalogError> {
        let lower = name.to_lowercase();
        let mut map = self.map.write();

        let (existing, reason) = map.get_visible_detailed(txn, &lower);
        if reason == LookupFailureReason::Success {
            if let Some(e) = existing {
                let ts = e.base.get_timestamp();
                if has_conflict(txn, ts) {
                    return Err(CatalogError::transaction_conflict(name));
                }
            }
            // 有现有条目 → 替换（推入链头）
            node.base.set_timestamp(txn.transaction_id);
            map.push_head(lower, node);
            return Ok(true);
        }
        // 否则走普通创建
        drop(map);
        self.create_entry(txn, name, node, dependencies)
    }

    /// Alter 条目（C++: `CatalogSet::AlterEntry`）。
    pub fn alter_entry(
        &self,
        txn: &CatalogTransaction,
        name: &str,
        info: &AlterInfo,
    ) -> Result<(), CatalogError> {
        let lower = name.to_lowercase();
        let mut map = self.map.write();

        let (existing, reason) = map.get_visible_detailed(txn, &lower);
        match reason {
            LookupFailureReason::NotPresent | LookupFailureReason::Invisible => {
                return Err(CatalogError::not_found(info.catalog_type, name));
            }
            LookupFailureReason::Deleted => {
                return Err(CatalogError::not_found(info.catalog_type, name));
            }
            LookupFailureReason::Success => {}
        }

        let existing = existing.unwrap();
        let ts = existing.base.get_timestamp();
        if has_conflict(txn, ts) {
            return Err(CatalogError::transaction_conflict(name));
        }

        // 生成新版本
        let mut new_node = Box::new(existing.alter(info)?);
        new_node.base.set_timestamp(txn.transaction_id);
        map.push_head(lower, new_node);
        Ok(())
    }

    /// Drop 条目（C++: `CatalogSet::DropEntry`）。
    pub fn drop_entry(
        &self,
        txn: &CatalogTransaction,
        name: &str,
        cascade: bool,
        allow_drop_internal: bool,
    ) -> Result<(), CatalogError> {
        let lower = name.to_lowercase();
        let mut map = self.map.write();

        let (existing, reason) = map.get_visible_detailed(txn, &lower);
        match reason {
            LookupFailureReason::NotPresent | LookupFailureReason::Invisible => {
                return Err(CatalogError::not_found(CatalogType::Invalid, name));
            }
            LookupFailureReason::Deleted => {
                return Err(CatalogError::not_found(CatalogType::Invalid, name));
            }
            LookupFailureReason::Success => {}
        }

        let existing = existing.unwrap();
        if existing.base.internal && !allow_drop_internal {
            return Err(CatalogError::invalid(
                format!("Cannot drop internal catalog entry \"{}\"", name)
            ));
        }

        let ts = existing.base.get_timestamp();
        if has_conflict(txn, ts) {
            return Err(CatalogError::transaction_conflict(name));
        }

        // 创建墓碑节点
        let oid = existing.base.oid;
        let entry_type = existing.base.entry_type;
        let catalog_name = existing.base.catalog_name.clone();
        let schema_name = existing.base.schema_name.clone();
        let mut tombstone = Box::new(CatalogEntryNode::tombstone(
            oid, name.to_string(), catalog_name, schema_name, entry_type
        ));
        tombstone.base.set_timestamp(txn.transaction_id);
        map.push_head(lower, tombstone);
        Ok(())
    }

    // ─── 读操作 ────────────────────────────────────────────────────────────────

    /// 获取对事务可见的条目（克隆）（C++: `CatalogSet::GetEntry`）。
    pub fn get_entry(&self, txn: &CatalogTransaction, name: &str) -> Option<CatalogEntryNode> {
        let lower = name.to_lowercase();
        let map = self.map.read();

        // 先尝试 map 中的条目
        if let Some(node) = map.get_visible(txn, &lower) {
            if !node.base.deleted {
                return Some(node.copy_current());
            }
            return None;
        }

        // 尝试默认生成器
        None  // 默认生成器在 get_entry_with_defaults 中处理
    }

    /// 带默认生成器的条目获取（需要 &mut self 因为默认生成器可能创建新条目）。
    pub fn get_entry_with_defaults(
        &mut self,
        txn: &CatalogTransaction,
        name: &str,
    ) -> Option<CatalogEntryNode> {
        if let Some(node) = self.get_entry(txn, name) {
            return Some(node);
        }

        // 尝试通过默认生成器创建
        if let Some(generator) = &mut self.defaults {
            if let Some(default_node) = generator.create_default_entry(txn, name) {
                let deps = LogicalDependencyList::new();
                let node_box = Box::new(default_node);
                // 将默认条目插入 map（使用系统事务时间戳 0）
                let mut sys_txn = *txn;
                sys_txn.transaction_id = 0;
                let _ = self.create_entry(&sys_txn, name, node_box, &deps);
                return self.get_entry(txn, name);
            }
        }
        None
    }

    /// 详细查找（C++: `CatalogSet::GetEntryDetailed`）。
    pub fn get_entry_detailed(&self, txn: &CatalogTransaction, name: &str) -> EntryLookupResult {
        let lower = name.to_lowercase();
        let map = self.map.read();
        let (node, reason) = map.get_visible_detailed(txn, &lower);
        EntryLookupResult {
            node: node.map(|n| Box::new(n.copy_current())),
            reason,
        }
    }

    // ─── MVCC 维护 ─────────────────────────────────────────────────────────────

    /// Undo 操作：回滚本事务对某条目的修改（C++: `CatalogSet::Undo`）。
    pub fn undo(&self, name: &str, _commit_id: TransactionId) {
        let lower = name.to_lowercase();
        let mut map = self.map.write();
        // 弹出链头（恢复到旧版本）
        map.pop_head(&lower);
    }

    /// 提交 Drop（C++: `CatalogSet::CommitDrop`）。
    ///
    /// 将墓碑节点的时间戳设置为 commit_id，使其对所有 start_time >= commit_id 的事务不可见。
    pub fn commit_drop(&self, name: &str, commit_id: TransactionId) {
        let lower = name.to_lowercase();
        let mut map = self.map.write();
        if let Some(head) = map.entries.get_mut(&lower) {
            if head.base.deleted {
                head.base.set_timestamp(commit_id);
            }
        }
    }

    /// 更新条目时间戳（提交时调用）（C++: `CatalogSet::UpdateTimestamp`）。
    pub fn update_timestamp(&self, name: &str, commit_id: TransactionId) {
        let lower = name.to_lowercase();
        let map = self.map.read();
        if let Some(head) = map.entries.get(&lower) {
            head.base.set_timestamp(commit_id);
        }
    }

    /// 清理不再可见的旧版本（C++: `CatalogSet::CleanupEntry`）。
    ///
    /// `oldest_active_start_time`：所有活跃事务中最旧的 start_time。
    /// 时间戳 < oldest_active_start_time 的旧版本可以安全删除。
    pub fn cleanup_entry(&self, name: &str, oldest_active_start_time: TransactionId) {
        let lower = name.to_lowercase();
        let mut map = self.map.write();
        if let Some(head) = map.entries.get_mut(&lower) {
            // 从链头开始，找到第一个时间戳 < oldest_active_start_time 的节点
            // 并截断其 older 链（那些旧版本不再需要）
            let mut current = head.as_mut();
            loop {
                let ts = current.base.get_timestamp();
                if ts < oldest_active_start_time {
                    // 这个节点及以下都不再需要
                    current.older = None;
                    break;
                }
                match current.older.as_deref_mut() {
                    Some(older) => current = older,
                    None => break,
                }
            }
        }
    }

    // ─── 遍历 ──────────────────────────────────────────────────────────────────

    /// 无事务约束地遍历所有链头（C++: `CatalogSet::Scan(function<void(CatalogEntry&)>)`）。
    pub fn scan<F>(&self, mut f: F)
    where
        F: FnMut(&CatalogEntryNode),
    {
        let map = self.map.read();
        for node in map.entries.values() {
            f(node.as_ref());
        }
    }

    /// 事务感知遍历（C++: `CatalogSet::Scan(CatalogTransaction, function<void(CatalogEntry&)>)`）。
    pub fn scan_with_txn<F>(&self, txn: &CatalogTransaction, mut f: F)
    where
        F: FnMut(&CatalogEntryNode),
    {
        let map = self.map.read();
        for head in map.entries.values() {
            // 找可见节点
            let mut current = head.as_ref();
            loop {
                if is_visible(txn, current.base.get_timestamp()) {
                    if !current.base.deleted {
                        f(current);
                    }
                    break;
                }
                match current.older.as_deref() {
                    Some(older) => current = older,
                    None => break,
                }
            }
        }
    }

    /// 前缀扫描（C++: `CatalogSet::ScanWithPrefix`）。
    pub fn scan_with_prefix<F>(&self, txn: &CatalogTransaction, prefix: &str, mut f: F)
    where
        F: FnMut(&CatalogEntryNode),
    {
        let lower_prefix = prefix.to_lowercase();
        let map = self.map.read();
        for (key, head) in map.entries.range(lower_prefix.clone()..) {
            if !key.starts_with(&lower_prefix) { break; }
            let mut current = head.as_ref();
            loop {
                if is_visible(txn, current.base.get_timestamp()) {
                    if !current.base.deleted {
                        f(current);
                    }
                    break;
                }
                match current.older.as_deref() {
                    Some(older) => current = older,
                    None => break,
                }
            }
        }
    }

    /// 可中断遍历（C++: `CatalogSet::ScanWithReturn`）。
    pub fn scan_with_return<F>(&self, txn: &CatalogTransaction, mut f: F) -> bool
    where
        F: FnMut(&CatalogEntryNode) -> bool,
    {
        let map = self.map.read();
        for head in map.entries.values() {
            let mut current = head.as_ref();
            loop {
                if is_visible(txn, current.base.get_timestamp()) {
                    if !current.base.deleted && !f(current) {
                        return false;
                    }
                    break;
                }
                match current.older.as_deref() {
                    Some(older) => current = older,
                    None => break,
                }
            }
        }
        true
    }

    // ─── 相似度查找 ────────────────────────────────────────────────────────────

    /// 查找最相似的条目名（C++: `CatalogSet::SimilarEntry`）。
    pub fn similar_entry(&self, txn: &CatalogTransaction, name: &str) -> SimilarCatalogEntry {
        let lower = name.to_lowercase();
        let map = self.map.read();
        let mut best = SimilarCatalogEntry::default();
        for (key, head) in map.entries.iter() {
            let mut current = head.as_ref();
            loop {
                if is_visible(txn, current.base.get_timestamp()) {
                    if !current.base.deleted {
                        let score = super::entry_lookup::string_similarity(&lower, key);
                        if score > best.score {
                            best = SimilarCatalogEntry::new(&current.base.name, score, None);
                        }
                    }
                    break;
                }
                match current.older.as_deref() {
                    Some(older) => current = older,
                    None => break,
                }
            }
        }
        best
    }

    // ─── 验证 ──────────────────────────────────────────────────────────────────

    /// 验证集合的一致性（C++: `CatalogSet::Verify`）。
    pub fn verify(&self) -> Result<(), CatalogError> {
        let map = self.map.read();
        for (key, head) in map.entries.iter() {
            let expected_key = head.base.name.to_lowercase();
            if *key != expected_key {
                return Err(CatalogError::invalid(format!(
                    "CatalogSet consistency error: key \"{}\" != name.lower \"{}\"",
                    key, expected_key
                )));
            }
        }
        Ok(())
    }

    /// 检查依赖是否存在（C++: `CatalogSet::VerifyExistenceOfDependency`）。
    pub fn verify_existence_of_dependency(
        &self,
        txn: &CatalogTransaction,
        dep_name: &str,
    ) -> Result<(), CatalogError> {
        let lower = dep_name.to_lowercase();
        let map = self.map.read();
        let (node, reason) = map.get_visible_detailed(txn, &lower);
        match reason {
            LookupFailureReason::Success => Ok(()),
            _ => Err(CatalogError::not_found(CatalogType::Invalid, dep_name)),
        }
    }

    // ─── 统计 ──────────────────────────────────────────────────────────────────

    /// 返回所有可见条目数量（包括已删除的链头）。
    pub fn entry_count(&self) -> usize {
        self.map.read().entries.len()
    }
}

impl Default for CatalogSet {
    fn default() -> Self { Self::new(0) }
}
