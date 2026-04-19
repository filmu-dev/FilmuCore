use std::fs;

use filmuvfs::proto::FileEntry;
use prost::Message;

#[test]
fn python_file_entry_fixture_decodes_with_fresh_direct_urls() {
    let fixture_path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/python_fresh_file_entry.hex"
    );
    let fixture_hex = fs::read_to_string(fixture_path).expect("fixture should be readable");
    let bytes = decode_hex(fixture_hex.trim());
    let entry = FileEntry::decode(bytes.as_slice()).expect("fixture should decode");

    assert_eq!(entry.locator, "https://edge.example.com/current-movie");
    assert_eq!(
        entry.restricted_url.as_deref(),
        Some("https://api.example.com/restricted/current-movie")
    );
    assert_eq!(
        entry.unrestricted_url.as_deref(),
        Some("https://edge.example.com/current-movie")
    );
    assert_eq!(entry.source_key.as_deref(), Some("persisted"));
    assert_eq!(
        entry.provider_file_id.as_deref(),
        Some("provider-file-movie-1")
    );
}

fn decode_hex(value: &str) -> Vec<u8> {
    assert_eq!(value.len() % 2, 0, "hex fixture length should be even");
    value
        .as_bytes()
        .chunks(2)
        .map(|chunk| {
            let text = std::str::from_utf8(chunk).expect("hex bytes should be utf-8");
            u8::from_str_radix(text, 16).expect("hex chunk should parse")
        })
        .collect()
}
