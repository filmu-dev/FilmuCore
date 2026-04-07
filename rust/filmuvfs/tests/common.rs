#![allow(dead_code)]

use std::sync::Arc;

use filmuvfs::{
    catalog::state::{inode_for_entry_id, CatalogStateStore},
    proto::{
        catalog_entry::Details as CatalogEntryDetails, CatalogDelta, CatalogEntry,
        CatalogEntryKind, CatalogFileTransport, CatalogLeaseState, CatalogLocatorSource,
        CatalogMatchBasis, CatalogMediaType, CatalogPlaybackRole, CatalogProviderFamily,
        CatalogRemoval, CatalogSnapshot, DirectoryEntry, FileEntry,
    },
};

pub const ROOT_ENTRY_ID: &str = "dir:/";
pub const MOVIES_ENTRY_ID: &str = "dir:/movies";
pub const SHOWS_ENTRY_ID: &str = "dir:/shows";
pub const MOVIE_DIR_ENTRY_ID: &str = "dir:/movies/Example Movie (2024)";
pub const SHOW_DIR_ENTRY_ID: &str = "dir:/shows/Example Show";
pub const SHOW_SEASON_ENTRY_ID: &str = "dir:/shows/Example Show/S01";
pub const MOVIE_FILE_ENTRY_ID: &str = "file:movie-1";
pub const EPISODE_FILE_ENTRY_ID: &str = "file:episode-1";

pub fn directory_entry(
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

pub fn file_entry(
    entry_id: &str,
    parent_entry_id: &str,
    path: &str,
    name: &str,
    media_type: CatalogMediaType,
    unrestricted_url: &str,
    provider_file_id: &str,
) -> CatalogEntry {
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
            item_external_ref: Some(format!("ext:{entry_id}")),
            media_entry_id: format!("media:{entry_id}"),
            source_attachment_id: None,
            media_type: media_type as i32,
            transport: CatalogFileTransport::RemoteDirect as i32,
            locator: unrestricted_url.to_owned(),
            local_path: None,
            restricted_url: None,
            unrestricted_url: Some(unrestricted_url.to_owned()),
            original_filename: Some(name.to_owned()),
            size_bytes: Some(1_024),
            lease_state: CatalogLeaseState::Ready as i32,
            expires_at: None,
            last_refreshed_at: None,
            last_refresh_error: None,
            provider: Some("realdebrid".to_owned()),
            provider_download_id: Some(format!("download:{entry_id}")),
            provider_file_id: Some(provider_file_id.to_owned()),
            provider_file_path: Some(format!("Media/{name}")),
            active_roles: vec![CatalogPlaybackRole::Direct as i32],
            source_key: Some(format!("source:{entry_id}")),
            query_strategy: Some("by-media-entry-id".to_owned()),
            provider_family: CatalogProviderFamily::Debrid as i32,
            locator_source: CatalogLocatorSource::UnrestrictedUrl as i32,
            match_basis: CatalogMatchBasis::ProviderFileId as i32,
            restricted_fallback: false,
        })),
    }
}

pub fn sample_catalog_snapshot(movie_url: &str, episode_url: &str) -> CatalogSnapshot {
    CatalogSnapshot {
        generation_id: "generation-1".to_owned(),
        entries: vec![
            directory_entry(ROOT_ENTRY_ID, None, "/", "/"),
            directory_entry(MOVIES_ENTRY_ID, Some(ROOT_ENTRY_ID), "/movies", "movies"),
            directory_entry(SHOWS_ENTRY_ID, Some(ROOT_ENTRY_ID), "/shows", "shows"),
            directory_entry(
                MOVIE_DIR_ENTRY_ID,
                Some(MOVIES_ENTRY_ID),
                "/movies/Example Movie (2024)",
                "Example Movie (2024)",
            ),
            directory_entry(
                SHOW_DIR_ENTRY_ID,
                Some(SHOWS_ENTRY_ID),
                "/shows/Example Show",
                "Example Show",
            ),
            directory_entry(
                SHOW_SEASON_ENTRY_ID,
                Some(SHOW_DIR_ENTRY_ID),
                "/shows/Example Show/S01",
                "S01",
            ),
            file_entry(
                MOVIE_FILE_ENTRY_ID,
                MOVIE_DIR_ENTRY_ID,
                "/movies/Example Movie (2024)/Example Movie (2024).mkv",
                "Example Movie (2024).mkv",
                CatalogMediaType::Movie,
                movie_url,
                "provider-file-movie-1",
            ),
            file_entry(
                EPISODE_FILE_ENTRY_ID,
                SHOW_SEASON_ENTRY_ID,
                "/shows/Example Show/S01/E01.mkv",
                "E01.mkv",
                CatalogMediaType::Episode,
                episode_url,
                "provider-file-episode-1",
            ),
        ],
        stats: None,
    }
}

pub fn add_second_movie_delta(movie_url: &str) -> CatalogDelta {
    CatalogDelta {
        generation_id: "generation-2".to_owned(),
        base_generation_id: Some("generation-1".to_owned()),
        upserts: vec![file_entry(
            "file:movie-2",
            MOVIE_DIR_ENTRY_ID,
            "/movies/Example Movie (2024)/Behind The Scenes.mkv",
            "Behind The Scenes.mkv",
            CatalogMediaType::Movie,
            movie_url,
            "provider-file-movie-2",
        )],
        removals: Vec::new(),
        stats: None,
    }
}

pub fn update_movie_url_delta(movie_url: &str) -> CatalogDelta {
    CatalogDelta {
        generation_id: "generation-2".to_owned(),
        base_generation_id: Some("generation-1".to_owned()),
        upserts: vec![file_entry(
            MOVIE_FILE_ENTRY_ID,
            MOVIE_DIR_ENTRY_ID,
            "/movies/Example Movie (2024)/Example Movie (2024).mkv",
            "Example Movie (2024).mkv",
            CatalogMediaType::Movie,
            movie_url,
            "provider-file-movie-1",
        )],
        removals: Vec::new(),
        stats: None,
    }
}

pub fn remove_movie_delta() -> CatalogDelta {
    CatalogDelta {
        generation_id: "generation-2".to_owned(),
        base_generation_id: Some("generation-1".to_owned()),
        upserts: Vec::new(),
        removals: vec![CatalogRemoval {
            entry_id: MOVIE_FILE_ENTRY_ID.to_owned(),
            path: "/movies/Example Movie (2024)/Example Movie (2024).mkv".to_owned(),
            kind: CatalogEntryKind::File as i32,
            correlation: None,
        }],
        stats: None,
    }
}

pub fn seeded_state(movie_url: &str, episode_url: &str) -> Arc<CatalogStateStore> {
    let state = Arc::new(CatalogStateStore::new());
    state
        .apply_snapshot(sample_catalog_snapshot(movie_url, episode_url))
        .expect("sample snapshot should apply");
    state
}

pub fn movie_inode() -> u64 {
    inode_for_entry_id(MOVIE_FILE_ENTRY_ID)
}

pub fn episode_inode() -> u64 {
    inode_for_entry_id(EPISODE_FILE_ENTRY_ID)
}
