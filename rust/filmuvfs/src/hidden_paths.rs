/// Returns true if this path component should be rejected with ENOENT without
/// consulting the catalog.
#[must_use]
pub fn is_hidden_path(name: &str) -> bool {
    if matches!(name, "." | "..") {
        return false;
    }

    let lower = name.trim().to_ascii_lowercase();
    if lower.is_empty() {
        return false;
    }

    const HIDDEN_EXACT: &[&str] = &[
        ".ds_store",
        ".spotlight-v100",
        ".trashes",
        ".fseventsd",
        ".localized",
        ".plexignore",
        ".embyignore",
        ".nomedia",
        "desktop.ini",
        "thumbs.db",
        ".hidden",
        ".metadata_never_index",
    ];

    if HIDDEN_EXACT.contains(&lower.as_str()) {
        return true;
    }

    if lower.starts_with("._") || lower.starts_with(".trash") {
        return true;
    }

    if lower.ends_with(".tmp") || lower.ends_with(".part") || lower.ends_with(".partial") {
        return true;
    }

    lower.starts_with('.')
}

/// Returns true if the full path should be ignored.
#[must_use]
pub fn is_ignored_path(path: &str) -> bool {
    let normalized = path.replace('\\', "/").to_ascii_lowercase();

    normalized.contains("/.plex versions/")
        || normalized.contains("/plex versions/")
        || normalized.contains("/.emby/")
        || normalized.contains("/metadata/")
        || normalized.contains("/@eadir/")
}
