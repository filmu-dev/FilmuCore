mod common;

use std::ffi::OsStr;

use filmuvfs::{
    catalog::state::{inode_for_entry_id, CatalogStateStore, ROOT_INODE},
    mount::{MountRuntime, MountRuntimeError},
};

#[test]
fn snapshot_application_produces_correct_inode_assignments() {
    let state = CatalogStateStore::new();
    state
        .apply_snapshot(common::sample_catalog_snapshot(
            "http://127.0.0.1:18080/movie.mkv",
            "http://127.0.0.1:18080/episode.mkv",
        ))
        .expect("snapshot should apply");

    let root = state
        .entry_by_inode(ROOT_INODE)
        .expect("root inode should resolve");
    let movie_inode = inode_for_entry_id(common::MOVIE_FILE_ENTRY_ID);
    let movie = state
        .entry_by_inode(movie_inode)
        .expect("movie inode should resolve");

    assert_eq!(root.entry_id, common::ROOT_ENTRY_ID);
    assert_eq!(root.path, "/");
    assert_eq!(movie.entry_id, common::MOVIE_FILE_ENTRY_ID);
    assert_eq!(
        movie.path,
        "/movies/Example Movie (2024)/Example Movie (2024).mkv"
    );
}

#[test]
fn delta_update_preserves_existing_inodes() {
    let state = CatalogStateStore::new();
    state
        .apply_snapshot(common::sample_catalog_snapshot(
            "http://127.0.0.1:18080/movie.mkv",
            "http://127.0.0.1:18080/episode.mkv",
        ))
        .expect("snapshot should apply");
    let original_movie_inode = common::movie_inode();

    state
        .apply_delta(common::add_second_movie_delta(
            "http://127.0.0.1:18080/behind-the-scenes.mkv",
        ))
        .expect("delta should apply");

    let movie_after_delta = state
        .entry_by_inode(original_movie_inode)
        .expect("existing movie inode should remain valid");
    let new_entry = state
        .entry("file:movie-2")
        .expect("new movie entry should exist after delta");

    assert_eq!(movie_after_delta.entry_id, common::MOVIE_FILE_ENTRY_ID);
    assert_ne!(
        inode_for_entry_id(&new_entry.entry_id),
        original_movie_inode
    );
}

#[test]
fn removal_invalidates_open_handle_correctly() {
    let state = common::seeded_state(
        "http://127.0.0.1:18080/movie.mkv",
        "http://127.0.0.1:18080/episode.mkv",
    );
    let runtime = MountRuntime::new(state.clone(), "session-test".to_owned());
    let movie_inode = common::movie_inode();
    let handle = runtime
        .open_by_inode(movie_inode)
        .expect("open_by_inode should succeed");

    state
        .apply_delta(common::remove_movie_delta())
        .expect("removal delta should apply");
    let invalidated = runtime.invalidate_handles_for_inodes(&[movie_inode]);

    assert_eq!(invalidated, 1);
    match runtime.prepare_read_request(handle.handle_id, movie_inode, 0, 64) {
        Err(MountRuntimeError::InodeNotFound { inode }) => assert_eq!(inode, movie_inode),
        other => panic!("expected invalidated handle to return inode-not-found, got {other:?}"),
    }

    let released = runtime
        .release(handle.handle_id)
        .expect("invalidated handle should still release cleanly");
    assert_eq!(released.handle_id, handle.handle_id);
}

#[test]
fn url_update_is_visible_to_subsequent_reads_without_reopen() {
    let state = common::seeded_state(
        "http://127.0.0.1:18080/movie-a.mkv",
        "http://127.0.0.1:18080/episode.mkv",
    );
    let runtime = MountRuntime::new(state.clone(), "session-test".to_owned());
    let movie_inode = common::movie_inode();
    let handle = runtime
        .open_by_inode(movie_inode)
        .expect("open_by_inode should succeed");

    state
        .apply_delta(common::update_movie_url_delta(
            "http://127.0.0.1:18080/movie-b.mkv",
        ))
        .expect("url update delta should apply");

    let request = runtime
        .prepare_read_request(handle.handle_id, movie_inode, 0, 64)
        .expect("read request should use refreshed catalog URL");
    assert_eq!(
        request.unrestricted_url,
        "http://127.0.0.1:18080/movie-b.mkv"
    );
}

#[test]
fn getattr_exposes_semantic_path_metadata_for_files() {
    let state = common::seeded_state(
        "http://127.0.0.1:18080/movie.mkv",
        "http://127.0.0.1:18080/episode.mkv",
    );
    let runtime = MountRuntime::new(state, "session-test".to_owned());

    let movie_attributes = runtime
        .getattr_by_inode(common::movie_inode())
        .expect("movie getattr should succeed");
    let episode_attributes = runtime
        .getattr_by_inode(common::episode_inode())
        .expect("episode getattr should succeed");

    assert_eq!(
        movie_attributes
            .semantic_path
            .path_type
            .map(|value| value.as_str()),
        Some("movie-file")
    );
    assert_eq!(
        movie_attributes.semantic_path.tmdb_id.as_deref(),
        Some("101")
    );
    assert_eq!(
        episode_attributes
            .semantic_path
            .path_type
            .map(|value| value.as_str()),
        Some("episode-file")
    );
    assert_eq!(
        episode_attributes.semantic_path.tvdb_id.as_deref(),
        Some("202")
    );
}

#[test]
fn readdir_and_open_surface_semantic_path_metadata() {
    let state = common::seeded_state(
        "http://127.0.0.1:18080/movie.mkv",
        "http://127.0.0.1:18080/episode.mkv",
    );
    let runtime = MountRuntime::new(state, "session-test".to_owned());

    let show_entries = runtime
        .readdir_by_inode(inode_for_entry_id(common::SHOW_DIR_ENTRY_ID))
        .expect("show directory listing should succeed");
    let season_directory = show_entries
        .iter()
        .find(|entry| entry.entry_id == common::SHOW_SEASON_ENTRY_ID)
        .expect("season directory should be listed");
    assert_eq!(
        season_directory
            .semantic_path
            .path_type
            .map(|value| value.as_str()),
        Some("show-season-directory")
    );
    assert_eq!(season_directory.semantic_path.season_number, Some(1));

    let handle = runtime
        .open_by_inode(common::episode_inode())
        .expect("episode open should succeed");
    assert_eq!(
        handle.semantic_path.path_type.map(|value| value.as_str()),
        Some("episode-file")
    );
    assert_eq!(handle.semantic_path.season_number, Some(1));
    assert_eq!(handle.semantic_path.episode_number, Some(1));
}

#[test]
fn getattr_resolves_external_ref_and_season_alias_paths() {
    let state = common::seeded_state(
        "http://127.0.0.1:18080/movie.mkv",
        "http://127.0.0.1:18080/episode.mkv",
    );
    let runtime = MountRuntime::new(state, "session-test".to_owned());

    let show_alias = runtime
        .getattr("/shows/tvdb-202")
        .expect("show external-ref alias should resolve");
    assert_eq!(show_alias.path, "/shows/Example Show");
    assert_eq!(show_alias.semantic_path.tvdb_id.as_deref(), Some("202"));

    let season_alias = runtime
        .getattr("/shows/Example Show/Season 01")
        .expect("season alias should resolve");
    assert_eq!(season_alias.path, "/shows/Example Show/S01");
    assert_eq!(season_alias.semantic_path.season_number, Some(1));
}

#[test]
fn open_resolves_episode_alias_path_to_canonical_catalog_file() {
    let state = common::seeded_state(
        "http://127.0.0.1:18080/movie.mkv",
        "http://127.0.0.1:18080/episode.mkv",
    );
    let runtime = MountRuntime::new(state, "session-test".to_owned());

    let handle = runtime
        .open("/shows/Example Show/Season 01/Episode 01.mkv")
        .expect("episode alias path should resolve");

    assert_eq!(handle.path, "/shows/Example Show/S01/E01.mkv");
    assert_eq!(
        handle.semantic_path.path_type.map(|value| value.as_str()),
        Some("episode-file")
    );
    assert_eq!(handle.semantic_path.tvdb_id.as_deref(), Some("202"));
    assert_eq!(handle.semantic_path.season_number, Some(1));
    assert_eq!(handle.semantic_path.episode_number, Some(1));
}

#[test]
fn readdir_surfaces_discoverable_alias_entries() {
    let state = common::seeded_state(
        "http://127.0.0.1:18080/movie.mkv",
        "http://127.0.0.1:18080/episode.mkv",
    );
    let runtime = MountRuntime::new(state, "session-test".to_owned());

    let show_root_entries = runtime
        .readdir_by_inode(inode_for_entry_id(common::SHOWS_ENTRY_ID))
        .expect("shows root listing should succeed");
    let show_alias = show_root_entries
        .iter()
        .find(|entry| entry.name == "tvdb-202")
        .expect("show alias entry should be listed");
    assert_eq!(show_alias.path, "/shows/tvdb-202");
    assert_eq!(
        show_alias.inode,
        inode_for_entry_id(common::SHOW_DIR_ENTRY_ID)
    );

    let movie_root_entries = runtime
        .readdir_by_inode(inode_for_entry_id(common::MOVIES_ENTRY_ID))
        .expect("movies root listing should succeed");
    let movie_alias = movie_root_entries
        .iter()
        .find(|entry| entry.name == "tmdb-101")
        .expect("movie alias entry should be listed");
    assert_eq!(movie_alias.path, "/movies/tmdb-101");
    assert_eq!(
        movie_alias.inode,
        inode_for_entry_id(common::MOVIE_DIR_ENTRY_ID)
    );

    let show_entries = runtime
        .readdir_by_inode(inode_for_entry_id(common::SHOW_DIR_ENTRY_ID))
        .expect("show directory listing should succeed");
    let season_alias = show_entries
        .iter()
        .find(|entry| entry.name == "Season 01")
        .expect("season alias entry should be listed");
    assert_eq!(season_alias.path, "/shows/Example Show/Season 01");
    assert_eq!(
        season_alias.inode,
        inode_for_entry_id(common::SHOW_SEASON_ENTRY_ID)
    );

    let season_entries = runtime
        .readdir_by_inode(inode_for_entry_id(common::SHOW_SEASON_ENTRY_ID))
        .expect("season directory listing should succeed");
    let episode_alias = season_entries
        .iter()
        .find(|entry| entry.name == "Episode 01.mkv")
        .expect("episode alias entry should be listed");
    assert_eq!(episode_alias.path, "/shows/Example Show/S01/Episode 01.mkv");
    assert_eq!(episode_alias.inode, common::episode_inode());
}

#[test]
fn lookup_by_inode_name_accepts_listed_aliases() {
    let state = common::seeded_state(
        "http://127.0.0.1:18080/movie.mkv",
        "http://127.0.0.1:18080/episode.mkv",
    );
    let runtime = MountRuntime::new(state, "session-test".to_owned());

    let show_alias = runtime
        .lookup_by_inode_name(
            inode_for_entry_id(common::SHOWS_ENTRY_ID),
            OsStr::new("tvdb-202"),
        )
        .expect("show alias lookup should resolve");
    assert_eq!(show_alias.path, "/shows/Example Show");

    let season_alias = runtime
        .lookup_by_inode_name(
            inode_for_entry_id(common::SHOW_DIR_ENTRY_ID),
            OsStr::new("Season 01"),
        )
        .expect("season alias lookup should resolve");
    assert_eq!(season_alias.path, "/shows/Example Show/S01");

    let episode_alias = runtime
        .lookup_by_inode_name(
            inode_for_entry_id(common::SHOW_SEASON_ENTRY_ID),
            OsStr::new("Episode 01.mkv"),
        )
        .expect("episode alias lookup should resolve");
    assert_eq!(episode_alias.path, "/shows/Example Show/S01/E01.mkv");
}
