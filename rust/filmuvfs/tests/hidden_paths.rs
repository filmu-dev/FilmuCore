use filmuvfs::hidden_paths::{is_hidden_path, is_ignored_path};

#[test]
fn test_ds_store_is_hidden() {
    assert!(is_hidden_path(".DS_Store"));
}

#[test]
fn test_spotlight_is_hidden() {
    assert!(is_hidden_path(".Spotlight-V100"));
}

#[test]
fn test_macos_resource_fork_is_hidden() {
    assert!(is_hidden_path("._movie.mkv"));
}

#[test]
fn test_plexignore_is_hidden() {
    assert!(is_hidden_path(".plexignore"));
}

#[test]
fn test_real_media_file_is_not_hidden() {
    assert!(!is_hidden_path("The.Movie.2024.mkv"));
    assert!(!is_hidden_path("Show.S01E01.mkv"));
}

#[test]
fn test_tmp_file_is_hidden() {
    assert!(is_hidden_path("upload.tmp"));
}

#[test]
fn test_ignored_plex_versions_path() {
    assert!(is_ignored_path("/movies/.Plex Versions/movie.mkv"));
}

#[test]
fn test_real_path_not_ignored() {
    assert!(!is_ignored_path("/movies/The.Movie.2024.mkv"));
}
