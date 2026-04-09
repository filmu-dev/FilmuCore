use std::{
    cmp::Ordering,
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, Ordering as AtomicOrdering},
        RwLock,
    },
};

use thiserror::Error;
use tracing::warn;

use crate::proto::{
    catalog_entry::Details as CatalogEntryDetails, CatalogDelta, CatalogEntry, CatalogEntryKind,
    CatalogRemoval, CatalogSnapshot,
};

pub const ROOT_INODE: u64 = 1;
const FNV_OFFSET_BASIS: u64 = 0xcbf29ce484222325;
const FNV_PRIME: u64 = 0x100000001b3;
const FALLBACK_INODE_START: u64 = 0x8000_0000_0000_0000;

static NEXT_FALLBACK_INODE: AtomicU64 = AtomicU64::new(FALLBACK_INODE_START);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct CatalogCounts {
    pub directories: usize,
    pub files: usize,
}

#[derive(Debug, Error)]
pub enum CatalogStateError {
    #[error("catalog entry {entry_id} has unsupported or missing details for kind {kind:?}")]
    InvalidEntry {
        entry_id: String,
        kind: CatalogEntryKind,
    },
    #[error(
        "catalog delta base generation mismatch: expected current generation {expected:?}, got {received:?}"
    )]
    GenerationMismatch {
        expected: Option<String>,
        received: Option<String>,
    },
    #[error("catalog path {path} is duplicated in the current snapshot or delta result")]
    DuplicatePath { path: String },
    #[error(
        "catalog inode collision detected for inode {inode} between entries {existing_entry_id} and {entry_id}"
    )]
    DuplicateInode {
        entry_id: String,
        existing_entry_id: String,
        inode: u64,
    },
}

#[derive(Debug, Default)]
struct CatalogStateInner {
    generation_id: Option<String>,
    counts: CatalogCounts,
    entries_by_id: HashMap<String, CatalogEntry>,
    path_to_entry_id: HashMap<String, String>,
    entry_id_to_inode: HashMap<String, u64>,
    inode_to_entry_id: HashMap<u64, String>,
    children_by_parent: HashMap<String, Vec<String>>,
}

#[derive(Debug, Default)]
pub struct CatalogStateStore {
    inner: RwLock<CatalogStateInner>,
}

impl CatalogStateStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn generation_id(&self) -> Option<String> {
        self.inner
            .read()
            .expect("catalog state lock poisoned")
            .generation_id
            .clone()
    }

    pub fn counts(&self) -> CatalogCounts {
        self.inner
            .read()
            .expect("catalog state lock poisoned")
            .counts
    }

    pub fn directory(&self, entry_id: &str) -> Option<CatalogEntry> {
        self.entry(entry_id)
            .filter(|entry| entry.kind() == CatalogEntryKind::Directory)
    }

    pub fn file(&self, entry_id: &str) -> Option<CatalogEntry> {
        self.entry(entry_id)
            .filter(|entry| entry.kind() == CatalogEntryKind::File)
    }

    pub fn entry(&self, entry_id: &str) -> Option<CatalogEntry> {
        self.inner
            .read()
            .expect("catalog state lock poisoned")
            .entries_by_id
            .get(entry_id)
            .cloned()
    }

    pub fn entry_by_path(&self, path: &str) -> Option<CatalogEntry> {
        let state = self.inner.read().expect("catalog state lock poisoned");
        state
            .path_to_entry_id
            .get(path)
            .and_then(|entry_id| state.entries_by_id.get(entry_id))
            .cloned()
    }

    pub fn entry_by_inode(&self, inode: u64) -> Option<CatalogEntry> {
        let state = self.inner.read().expect("catalog state lock poisoned");
        state
            .inode_to_entry_id
            .get(&inode)
            .and_then(|entry_id| state.entries_by_id.get(entry_id))
            .cloned()
    }

    pub fn inode_for_entry_id(&self, entry_id: &str) -> Option<u64> {
        self.inner
            .read()
            .expect("catalog state lock poisoned")
            .entry_id_to_inode
            .get(entry_id)
            .copied()
    }

    pub fn entries(&self) -> Vec<CatalogEntry> {
        let state = self.inner.read().expect("catalog state lock poisoned");
        let mut entries: Vec<CatalogEntry> = state.entries_by_id.values().cloned().collect();
        entries.sort_by(compare_entries);
        entries
    }

    pub fn children_of(&self, parent_entry_id: &str) -> Vec<CatalogEntry> {
        let state = self.inner.read().expect("catalog state lock poisoned");
        state
            .children_by_parent
            .get(parent_entry_id)
            .into_iter()
            .flat_map(|children| children.iter())
            .filter_map(|child_id| state.entries_by_id.get(child_id))
            .cloned()
            .collect()
    }

    pub fn apply_snapshot(&self, snapshot: CatalogSnapshot) -> Result<(), CatalogStateError> {
        let existing_inodes = self
            .inner
            .read()
            .expect("catalog state lock poisoned")
            .entry_id_to_inode
            .clone();
        let new_state = build_state_with_existing(
            snapshot.entries,
            Some(snapshot.generation_id),
            Some(&existing_inodes),
        )?;
        *self.inner.write().expect("catalog state lock poisoned") = new_state;
        Ok(())
    }

    pub fn apply_delta(&self, delta: CatalogDelta) -> Result<(), CatalogStateError> {
        let mut state = self.inner.write().expect("catalog state lock poisoned");
        if let Some(expected_base) = delta.base_generation_id.as_ref() {
            if state.generation_id.as_ref() != Some(expected_base) {
                return Err(CatalogStateError::GenerationMismatch {
                    expected: state.generation_id.clone(),
                    received: Some(expected_base.clone()),
                });
            }
        }

        let mut entries_by_id = state.entries_by_id.clone();
        for entry in delta.upserts {
            validate_entry(&entry)?;
            entries_by_id.insert(entry.entry_id.clone(), entry);
        }

        for removal in delta.removals {
            remove_entry_and_descendants(&mut entries_by_id, &removal);
        }

        let existing_inodes = state.entry_id_to_inode.clone();
        *state = build_state_with_existing(
            entries_by_id.into_values().collect(),
            Some(delta.generation_id),
            Some(&existing_inodes),
        )?;
        Ok(())
    }

    pub fn update_file_unrestricted_url(&self, entry_id: &str, new_url: String) -> bool {
        let mut state = self.inner.write().expect("catalog state lock poisoned");
        let Some(entry) = state.entries_by_id.get_mut(entry_id) else {
            return false;
        };

        match entry.details.as_mut() {
            Some(CatalogEntryDetails::File(file)) => {
                let previous_unrestricted_url = file.unrestricted_url.clone();
                file.unrestricted_url = Some(new_url.clone());
                if previous_unrestricted_url.as_deref() == Some(file.locator.as_str()) {
                    file.locator = new_url;
                }
                true
            }
            _ => false,
        }
    }
}

pub fn inode_for_entry_id(entry_id: &str) -> u64 {
    if entry_id == "dir:/" {
        return ROOT_INODE;
    }

    let mut hash = FNV_OFFSET_BASIS;
    for byte in entry_id.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }

    if hash == 0 || hash == ROOT_INODE {
        hash = hash.wrapping_add(2);
    }

    hash
}

fn build_state_with_existing(
    entries: Vec<CatalogEntry>,
    generation_id: Option<String>,
    existing_entry_id_to_inode: Option<&HashMap<String, u64>>,
) -> Result<CatalogStateInner, CatalogStateError> {
    build_state_with_inode_resolver(
        entries,
        generation_id,
        existing_entry_id_to_inode,
        inode_for_entry_id,
    )
}

fn build_state_with_inode_resolver<F>(
    entries: Vec<CatalogEntry>,
    generation_id: Option<String>,
    existing_entry_id_to_inode: Option<&HashMap<String, u64>>,
    mut inode_resolver: F,
) -> Result<CatalogStateInner, CatalogStateError>
where
    F: FnMut(&str) -> u64,
{
    let mut entries_by_id = HashMap::<String, CatalogEntry>::new();
    for entry in entries {
        validate_entry(&entry)?;
        entries_by_id.insert(entry.entry_id.clone(), entry);
    }

    let mut ordered_entries: Vec<CatalogEntry> = entries_by_id.values().cloned().collect();
    ordered_entries.sort_by(compare_entries);

    let mut counts = CatalogCounts::default();
    let mut path_to_entry_id = HashMap::<String, String>::new();
    let mut entry_id_to_inode = HashMap::<String, u64>::new();
    let mut inode_to_entry_id = HashMap::<u64, String>::new();
    let mut children_by_parent = HashMap::<String, Vec<String>>::new();

    for entry in &ordered_entries {
        match entry.kind() {
            CatalogEntryKind::Directory => counts.directories += 1,
            CatalogEntryKind::File => counts.files += 1,
            CatalogEntryKind::Unspecified => {
                return Err(CatalogStateError::InvalidEntry {
                    entry_id: entry.entry_id.clone(),
                    kind: entry.kind(),
                });
            }
        }

        if path_to_entry_id
            .insert(entry.path.clone(), entry.entry_id.clone())
            .is_some()
        {
            return Err(CatalogStateError::DuplicatePath {
                path: entry.path.clone(),
            });
        }

        let preferred_inode = existing_entry_id_to_inode
            .and_then(|mapping| mapping.get(&entry.entry_id).copied())
            .unwrap_or_else(|| inode_resolver(&entry.entry_id));
        let assigned_inode =
            if let Some(existing_entry_id) = inode_to_entry_id.get(&preferred_inode) {
                if existing_entry_id != &entry.entry_id {
                    warn!(
                        entry_id = %entry.entry_id,
                        existing_entry_id = %existing_entry_id,
                        inode = preferred_inode,
                        "catalog inode collision detected; allocating fallback inode"
                    );
                    allocate_fallback_inode(&inode_to_entry_id, &entry.entry_id)
                } else {
                    preferred_inode
                }
            } else {
                preferred_inode
            };
        inode_to_entry_id.insert(assigned_inode, entry.entry_id.clone());
        entry_id_to_inode.insert(entry.entry_id.clone(), assigned_inode);

        if let Some(parent_entry_id) = entry.parent_entry_id.as_ref() {
            children_by_parent
                .entry(parent_entry_id.clone())
                .or_default()
                .push(entry.entry_id.clone());
            continue;
        }

        if let Some(parent_path) = parent_path_from_catalog_path(&entry.path) {
            if let Some(parent_entry_id) = path_to_entry_id.get(parent_path) {
                children_by_parent
                    .entry(parent_entry_id.clone())
                    .or_default()
                    .push(entry.entry_id.clone());
            }
        }
    }

    for child_entry_ids in children_by_parent.values_mut() {
        child_entry_ids.sort_by(|left_id, right_id| {
            let left = entries_by_id
                .get(left_id)
                .expect("child entry id must exist during state build");
            let right = entries_by_id
                .get(right_id)
                .expect("child entry id must exist during state build");
            compare_entries(left, right)
        });
    }

    Ok(CatalogStateInner {
        generation_id,
        counts,
        entries_by_id,
        path_to_entry_id,
        entry_id_to_inode,
        inode_to_entry_id,
        children_by_parent,
    })
}

fn allocate_fallback_inode(inode_to_entry_id: &HashMap<u64, String>, entry_id: &str) -> u64 {
    loop {
        let candidate = NEXT_FALLBACK_INODE.fetch_add(1, AtomicOrdering::Relaxed);
        if candidate == 0 || candidate == ROOT_INODE {
            continue;
        }
        if !inode_to_entry_id.contains_key(&candidate) {
            return candidate;
        }
        warn!(
            entry_id = %entry_id,
            inode = candidate,
            "catalog fallback inode already allocated; probing next candidate"
        );
    }
}

fn parent_path_from_catalog_path(path: &str) -> Option<&str> {
    if path == "/" {
        return None;
    }

    let trimmed = path.trim_end_matches('/');
    if trimmed.is_empty() || trimmed == "/" {
        return Some("/");
    }

    match trimmed.rfind('/') {
        Some(0) => Some("/"),
        Some(index) => Some(&trimmed[..index]),
        None => Some("/"),
    }
}

fn validate_entry(entry: &CatalogEntry) -> Result<(), CatalogStateError> {
    let kind = entry.kind();
    match kind {
        CatalogEntryKind::Directory => {
            if matches!(entry.details, Some(CatalogEntryDetails::Directory(_))) {
                Ok(())
            } else {
                Err(CatalogStateError::InvalidEntry {
                    entry_id: entry.entry_id.clone(),
                    kind,
                })
            }
        }
        CatalogEntryKind::File => {
            if matches!(entry.details, Some(CatalogEntryDetails::File(_))) {
                Ok(())
            } else {
                Err(CatalogStateError::InvalidEntry {
                    entry_id: entry.entry_id.clone(),
                    kind,
                })
            }
        }
        CatalogEntryKind::Unspecified => Err(CatalogStateError::InvalidEntry {
            entry_id: entry.entry_id.clone(),
            kind,
        }),
    }
}

fn remove_entry_and_descendants(
    entries_by_id: &mut HashMap<String, CatalogEntry>,
    removal: &CatalogRemoval,
) {
    if removal.path == "/" {
        entries_by_id.clear();
        return;
    }

    let removal_prefix = format!("{}/", removal.path.trim_end_matches('/'));
    entries_by_id.retain(|entry_id, entry| {
        entry_id != &removal.entry_id
            && entry.path != removal.path
            && !entry.path.starts_with(&removal_prefix)
    });
}

fn compare_entries(left: &CatalogEntry, right: &CatalogEntry) -> Ordering {
    left.kind()
        .cmp(&right.kind())
        .then_with(|| left.name.to_lowercase().cmp(&right.name.to_lowercase()))
        .then_with(|| left.path.cmp(&right.path))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::{
        catalog_entry::Details as CatalogEntryDetails, CatalogEntry, CatalogEntryKind,
        DirectoryEntry, FileEntry,
    };

    fn directory_entry(
        entry_id: &str,
        parent_entry_id: Option<&str>,
        path: &str,
        name: &str,
    ) -> CatalogEntry {
        CatalogEntry {
            entry_id: entry_id.to_owned(),
            parent_entry_id: parent_entry_id.map(str::to_owned),
            path: path.to_owned(),
            name: name.to_owned(),
            kind: CatalogEntryKind::Directory as i32,
            correlation: None,
            details: Some(CatalogEntryDetails::Directory(DirectoryEntry {})),
        }
    }

    fn file_entry(entry_id: &str, parent_entry_id: &str, path: &str, name: &str) -> CatalogEntry {
        CatalogEntry {
            entry_id: entry_id.to_owned(),
            parent_entry_id: Some(parent_entry_id.to_owned()),
            path: path.to_owned(),
            name: name.to_owned(),
            kind: CatalogEntryKind::File as i32,
            correlation: None,
            details: Some(CatalogEntryDetails::File(FileEntry {
                item_id: format!("item:{entry_id}"),
                item_title: name.to_owned(),
                media_entry_id: format!("media:{entry_id}"),
                transport: 2,
                locator: format!("https://cdn.example.com/{entry_id}"),
                lease_state: 1,
                unrestricted_url: Some(format!("https://cdn.example.com/{entry_id}")),
                ..FileEntry::default()
            })),
        }
    }

    #[test]
    fn inode_collisions_allocate_fallback_inodes_without_corrupting_state() {
        let state = build_state_with_inode_resolver(
            vec![
                directory_entry("dir:/", None, "/", "/"),
                file_entry("file:one", "dir:/", "/one.mkv", "one.mkv"),
                file_entry("file:two", "dir:/", "/two.mkv", "two.mkv"),
            ],
            Some("generation-1".to_owned()),
            None,
            |entry_id| {
                if entry_id == "dir:/" {
                    ROOT_INODE
                } else {
                    42
                }
            },
        )
        .expect("state build should succeed when the preferred inode collides");

        let first_inode = state
            .entry_id_to_inode
            .get("file:one")
            .copied()
            .expect("first file inode should exist");
        let second_inode = state
            .entry_id_to_inode
            .get("file:two")
            .copied()
            .expect("second file inode should exist");

        assert_eq!(first_inode, 42);
        assert_ne!(second_inode, 42);
        assert_eq!(
            state
                .inode_to_entry_id
                .get(&first_inode)
                .map(String::as_str),
            Some("file:one")
        );
        assert_eq!(
            state
                .inode_to_entry_id
                .get(&second_inode)
                .map(String::as_str),
            Some("file:two")
        );
    }
}
