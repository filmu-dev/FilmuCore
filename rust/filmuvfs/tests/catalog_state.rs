mod common;

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
