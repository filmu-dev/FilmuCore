use std::fmt;

const ROOT_PATH: &str = "/";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MediaSemanticPathType {
    Root,
    MoviesRoot,
    MovieDirectory,
    MovieFile,
    ShowsRoot,
    ShowDirectory,
    ShowSeasonDirectory,
    ShowSpecialsDirectory,
    ShowFile,
    EpisodeFile,
    Unknown,
}

impl MediaSemanticPathType {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Root => "root",
            Self::MoviesRoot => "movies-root",
            Self::MovieDirectory => "movie-directory",
            Self::MovieFile => "movie-file",
            Self::ShowsRoot => "shows-root",
            Self::ShowDirectory => "show-directory",
            Self::ShowSeasonDirectory => "show-season-directory",
            Self::ShowSpecialsDirectory => "show-specials-directory",
            Self::ShowFile => "show-file",
            Self::EpisodeFile => "episode-file",
            Self::Unknown => "unknown",
        }
    }
}

impl fmt::Display for MediaSemanticPathType {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct MediaSemanticPathInfo {
    pub path_type: Option<MediaSemanticPathType>,
    pub tmdb_id: Option<String>,
    pub tvdb_id: Option<String>,
    pub imdb_id: Option<String>,
    pub season_number: Option<u32>,
    pub episode_number: Option<u32>,
}

#[must_use]
pub fn parse_media_semantic_path(
    path: &str,
    item_external_ref: Option<&str>,
) -> MediaSemanticPathInfo {
    let normalized_path = normalize_path(path);
    let segments: Vec<&str> = normalized_path
        .split('/')
        .filter(|segment| !segment.is_empty())
        .collect();

    let mut info = MediaSemanticPathInfo {
        path_type: Some(classify_path_type(&segments)),
        ..MediaSemanticPathInfo::default()
    };
    populate_external_ref_fields(&mut info, item_external_ref);

    if let Some(season_number) = infer_season_number(&segments) {
        info.season_number = Some(season_number);
    }
    if let Some(episode_number) = infer_episode_number(&segments) {
        info.episode_number = Some(episode_number);
    }

    if info.episode_number.is_some()
        && matches!(
            info.path_type,
            Some(MediaSemanticPathType::ShowFile) | Some(MediaSemanticPathType::Unknown)
        )
    {
        info.path_type = Some(MediaSemanticPathType::EpisodeFile);
    }

    info
}

fn normalize_path(path: &str) -> String {
    let trimmed = path.trim();
    if trimmed.is_empty() || trimmed == ROOT_PATH {
        return ROOT_PATH.to_owned();
    }

    let normalized_segments: Vec<&str> = trimmed
        .split('/')
        .filter(|segment| !segment.is_empty())
        .collect();
    format!("/{}", normalized_segments.join("/"))
}

fn classify_path_type(segments: &[&str]) -> MediaSemanticPathType {
    match segments {
        [] => MediaSemanticPathType::Root,
        ["movies"] => MediaSemanticPathType::MoviesRoot,
        ["movies", _movie_dir] => MediaSemanticPathType::MovieDirectory,
        ["movies", _movie_dir, _file_name] => MediaSemanticPathType::MovieFile,
        ["shows"] => MediaSemanticPathType::ShowsRoot,
        ["shows", _show_dir] => MediaSemanticPathType::ShowDirectory,
        ["shows", _show_dir, third] if is_specials_segment(third) => {
            MediaSemanticPathType::ShowSpecialsDirectory
        }
        ["shows", _show_dir, third] if parse_season_segment(third).is_some() => {
            MediaSemanticPathType::ShowSeasonDirectory
        }
        ["shows", _show_dir, third, _file_name] if is_specials_segment(third) => {
            MediaSemanticPathType::EpisodeFile
        }
        ["shows", _show_dir, third, _file_name] if parse_season_segment(third).is_some() => {
            MediaSemanticPathType::EpisodeFile
        }
        ["shows", _show_dir, _file_name] => MediaSemanticPathType::ShowFile,
        _ => MediaSemanticPathType::Unknown,
    }
}

fn populate_external_ref_fields(info: &mut MediaSemanticPathInfo, item_external_ref: Option<&str>) {
    let Some(raw_external_ref) = item_external_ref else {
        return;
    };
    let trimmed = raw_external_ref.trim();
    if trimmed.is_empty() {
        return;
    }

    let Some((prefix, value)) = trimmed.split_once(':') else {
        return;
    };
    let normalized_value = value.trim();
    if normalized_value.is_empty() {
        return;
    }

    match prefix.trim().to_ascii_lowercase().as_str() {
        "tmdb" => info.tmdb_id = Some(normalized_value.to_owned()),
        "tvdb" => info.tvdb_id = Some(normalized_value.to_owned()),
        "imdb" => info.imdb_id = Some(normalized_value.to_owned()),
        _ => {}
    }
}

fn infer_season_number(segments: &[&str]) -> Option<u32> {
    for segment in segments.iter().rev() {
        if is_specials_segment(segment) {
            return Some(0);
        }
        if let Some(season_number) = parse_season_segment(segment) {
            return Some(season_number);
        }
    }

    segments
        .last()
        .and_then(|segment| parse_filename_token(segment, &["s"], 2))
        .or_else(|| segments.last().and_then(|segment| parse_x_notation(segment).map(|(season, _)| season)))
}

fn infer_episode_number(segments: &[&str]) -> Option<u32> {
    segments
        .last()
        .and_then(|segment| parse_episode_from_filename(segment))
}

fn parse_episode_from_filename(file_name: &str) -> Option<u32> {
    parse_season_episode_token(file_name)
        .map(|(_, episode_number)| episode_number)
        .or_else(|| parse_x_notation(file_name).map(|(_, episode_number)| episode_number))
        .or_else(|| parse_episode_word_number(file_name))
}

fn parse_season_segment(segment: &str) -> Option<u32> {
    let normalized = sanitize_token(segment);
    if normalized.starts_with("season") {
        return normalized
            .trim_start_matches("season")
            .parse::<u32>()
            .ok();
    }
    if normalized.starts_with('s') && normalized.len() > 1 {
        return normalized[1..].parse::<u32>().ok();
    }
    None
}

fn parse_season_episode_token(file_name: &str) -> Option<(u32, u32)> {
    let normalized = sanitize_token(file_name);
    let bytes = normalized.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] != b's' {
            index += 1;
            continue;
        }

        let Some(season_end) = consume_digits(bytes, index + 1, 2) else {
            index += 1;
            continue;
        };
        if season_end >= bytes.len() || bytes[season_end] != b'e' {
            index += 1;
            continue;
        }
        let Some(episode_end) = consume_digits(bytes, season_end + 1, 3) else {
            index += 1;
            continue;
        };
        let season = normalized[index + 1..season_end].parse::<u32>().ok()?;
        let episode = normalized[season_end + 1..episode_end].parse::<u32>().ok()?;
        return Some((season, episode));
    }
    None
}

fn parse_x_notation(file_name: &str) -> Option<(u32, u32)> {
    let normalized = sanitize_token(file_name);
    let bytes = normalized.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        if !bytes[index].is_ascii_digit() {
            index += 1;
            continue;
        }
        let Some(season_end) = consume_digits(bytes, index, 2) else {
            index += 1;
            continue;
        };
        if season_end >= bytes.len() || bytes[season_end] != b'x' {
            index += 1;
            continue;
        }
        let Some(episode_end) = consume_digits(bytes, season_end + 1, 3) else {
            index += 1;
            continue;
        };
        let season = normalized[index..season_end].parse::<u32>().ok()?;
        let episode = normalized[season_end + 1..episode_end].parse::<u32>().ok()?;
        return Some((season, episode));
    }
    None
}

fn parse_episode_word_number(file_name: &str) -> Option<u32> {
    let normalized = sanitize_token(file_name);
    for needle in ["episode", "ep", "e"] {
        if let Some(start) = normalized.find(needle) {
            let number_start = start + needle.len();
            let Some(number_end) = consume_digits(normalized.as_bytes(), number_start, 3) else {
                continue;
            };
            return normalized[number_start..number_end].parse::<u32>().ok();
        }
    }
    None
}

fn parse_filename_token(file_name: &str, prefixes: &[&str], max_digits: usize) -> Option<u32> {
    let normalized = sanitize_token(file_name);
    for prefix in prefixes {
        if let Some(index) = normalized.find(prefix) {
            let number_start = index + prefix.len();
            let Some(number_end) =
                consume_digits(normalized.as_bytes(), number_start, max_digits)
            else {
                continue;
            };
            return normalized[number_start..number_end].parse::<u32>().ok();
        }
    }
    None
}

fn consume_digits(bytes: &[u8], start: usize, max_digits: usize) -> Option<usize> {
    if start >= bytes.len() || !bytes[start].is_ascii_digit() {
        return None;
    }
    let mut end = start;
    while end < bytes.len() && bytes[end].is_ascii_digit() && end - start < max_digits {
        end += 1;
    }
    Some(end)
}

fn sanitize_token(value: &str) -> String {
    value
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() {
                character.to_ascii_lowercase()
            } else {
                ' '
            }
        })
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join("")
}

fn is_specials_segment(segment: &str) -> bool {
    sanitize_token(segment).contains("specials")
}

#[cfg(test)]
mod tests {
    use super::{
        parse_media_semantic_path, MediaSemanticPathInfo, MediaSemanticPathType,
    };

    #[test]
    fn parses_movie_file_path_and_tmdb_external_ref() {
        let info = parse_media_semantic_path(
            "/movies/Mount Movie (2024)/Mount Movie.mkv",
            Some("tmdb:12345"),
        );

        assert_eq!(
            info,
            MediaSemanticPathInfo {
                path_type: Some(MediaSemanticPathType::MovieFile),
                tmdb_id: Some("12345".to_owned()),
                tvdb_id: None,
                imdb_id: None,
                season_number: None,
                episode_number: None,
            }
        );
    }

    #[test]
    fn parses_episode_file_from_season_directory_and_tvdb_external_ref() {
        let info = parse_media_semantic_path(
            "/shows/Stranger Things (2016)/Season 05/Stranger Things - S05E08.mkv",
            Some("tvdb:305288"),
        );

        assert_eq!(info.path_type, Some(MediaSemanticPathType::EpisodeFile));
        assert_eq!(info.tvdb_id.as_deref(), Some("305288"));
        assert_eq!(info.season_number, Some(5));
        assert_eq!(info.episode_number, Some(8));
    }

    #[test]
    fn parses_show_file_when_episode_metadata_lives_in_filename_only() {
        let info = parse_media_semantic_path(
            "/shows/Frieren_ Beyond Journey's End/Frieren - Episode 04.mkv",
            Some("tvdb:424536"),
        );

        assert_eq!(info.path_type, Some(MediaSemanticPathType::EpisodeFile));
        assert_eq!(info.tvdb_id.as_deref(), Some("424536"));
        assert_eq!(info.season_number, None);
        assert_eq!(info.episode_number, Some(4));
    }

    #[test]
    fn parses_specials_paths_as_season_zero() {
        let info = parse_media_semantic_path(
            "/shows/Specials Show/Specials/Specials Show - Episode 01.mkv",
            Some("tvdb:900002"),
        );

        assert_eq!(info.path_type, Some(MediaSemanticPathType::EpisodeFile));
        assert_eq!(info.tvdb_id.as_deref(), Some("900002"));
        assert_eq!(info.season_number, Some(0));
        assert_eq!(info.episode_number, Some(1));
    }

    #[test]
    fn leaves_unknown_external_refs_unset() {
        let info = parse_media_semantic_path("/shows/Example Show", Some("custom:abc"));

        assert_eq!(info.path_type, Some(MediaSemanticPathType::ShowDirectory));
        assert!(info.tmdb_id.is_none());
        assert!(info.tvdb_id.is_none());
        assert!(info.imdb_id.is_none());
    }
}
