# RTN (Rank Torrent Name) — riven-ts Implementation Report

> Source: `Triven_backend - ts/packages/util-rank-torrent-name`
> Integration: `Triven_backend - ts/apps/riven/lib/message-queue/flows/`

This package is still a live concern after the April 11, 2026 re-audit: the local `Triven_backend - ts` comparison checkout is now aligned directly to upstream `main` at `f98cc31`, and it includes `@repo/util-rank-torrent-name` as part of the active app dependency graph.

---

## 1. Package Architecture

The RTN logic lives in an isolated turborepo package (`@repo/util-rank-torrent-name`) consumed by the main `riven` app via workspace dependency. The package is structured into three layers:

| Layer | Directory | Purpose |
|---|---|---|
| **Parser** | `lib/parser/` | Extracts structured metadata from raw torrent names |
| **Ranker** | `lib/ranker/` | Scores, filters, and validates parsed torrents |
| **Shared** | `lib/shared/` | Mappings, normalisation, and language groups |

**Entry class**: `RTN` (`lib/rtn.ts`) — a thin facade that holds compiled settings, a ranking model, and enabled resolutions. Exposes two methods: `rankTorrent()` and `sortTorrents()`.

---

## 2. Parsing Layer

### Core Parser (`lib/parser/parse.ts`)

Built on `@viren070/parse-torrent-title` (a fork of `parse-torrent-title`) with custom handler chains added **before and after** the default handlers:

1. **Pre-default handlers** (run first):
   - `adultHandlers` — 1,084 lines containing ~560 adult studio/keyword patterns in a `Set`, matched case-insensitively against the raw title.
   - `sceneHandlers` — Detects scene releases via a regex matching known scene group tags (e.g., `-CAKES`, `-GGEZ`, `-GOSSIP`) and a WEB non-DL pattern.
   - `trashHandlers` — 13 regex patterns catching CAM/TS/Screener/DVB/SAT/Leaked/R5/R6/deleted-scenes/HQ-clean-audio variants.
   - Custom `channels` handler — Extracts `2.0` channel info.
   - Custom `complete` handler — Detects "complete series/seasons/collection" packs.

2. **Default handlers** (from `@viren070/parse-torrent-title`) — extracts title, year, resolution, quality, codec, seasons, episodes, audio, HDR, languages, etc.

3. **Post-default handlers** (run last):
   - Anime-specific episode extractor — For titles matching `One.*?Piece|Bleach|Naruto`, extracts bare episode numbers.
   - Bitrate extractor — Matches `\d+[kmg]bps`.
   - Site extractor — Matches `rarbg|torrentleech|piratebay`.

### Schema Validation (`lib/schemas.ts`)

All parsed output is validated through a Zod `ParsedDataSchema` with 40+ fields, then **transformed** to add:
- `type`: `"movie"` or `"show"` (derived from presence of seasons/episodes).
- `normalisedTitle`: Via the normaliser (see §5).
- `converted`: Boolean from `convert` field.
- `remux`: Boolean from `remux` field or quality string matching.

**Resolution enum**: `2160p | 1440p | 1080p | 720p | 480p | 360p | unknown`.
**ResolutionRank**: Auto-generated reverse-index (2160p=7, 1440p=6, …, unknown=1) used for tiebreaking.

---

## 3. Ranking Layer

### Score Calculation (`lib/ranker/rank.ts`)

The `rank()` function computes a total score from additive parts:

| Score Part | Source | Lookup Method |
|---|---|---|
| `quality` | `data.quality` | Single value → `QUALITY_MAP` |
| `codec` | `data.codec` | Single value → `CODEC_MAP` |
| `hdr` | `data.hdr[]` | List → `HDR_MAP` (summed) |
| `bitDepth` | `data.bitDepth` | Boolean → `hdr.bit10` rank |
| `audio` | `data.audio[]` | List → `AUDIO_MAP` (summed) |
| `channels` | `data.channels[]` | List → `CHANNEL_MAP` (summed) |
| `flags` | Boolean fields on `data` | Each true flag → `FLAG_MAP` (summed) |
| `preferredPatterns` | `rawTitle` vs compiled regexes | Match → **+10,000** |
| `preferredLanguages` | `data.languages` vs preferred list | Match → **+10,000** |

Each mapped value resolves through `resolveRank()`: if the user's `customRanks` for that `[category, key]` pair has a `.rank` override, it uses that; otherwise it falls back to the `RankingModel` default.

### `rankTorrent()` — Full Pipeline

1. **Hash validation** — SHA-1 via Zod `z.hash("sha1")`.
2. **Title similarity** — Levenshtein ratio (see §5) against correct title + aliases.
3. If `removeAllTrash` is on and similarity < threshold → `TitleSimilarityError`.
4. **Score** — `rank(data, settings, rankingModel)`.
5. **Fetch checks** — `checkFetch(data, settings)` (see §4).
6. If `removeAllTrash` and fetch failed → `FetchChecksFailedError`.
7. If `removeAllTrash` and score < `removeRanksUnder` → `RankUnderThresholdError`.
8. Returns `RankedResult` with `data`, `hash`, `rank`, `levRatio`, `fetch`, `failedChecks`, `scoreParts`.

---

## 4. Fetch-Check Pipeline (`lib/ranker/fetch.ts`)

`checkFetch()` runs 8 sequential checks, accumulating failed reasons into a `Set<string>`:

| # | Check | Rejects When |
|---|---|---|
| 1 | `trashHandler` | Quality is CAM/TS/Screener/etc., audio is "HQ Clean Audio", or `trash` flag is set |
| 2 | `adultHandler` | `adult` flag true and `removeAdultContent` enabled |
| 3 | `checkExclude` | `rawTitle` matches any compiled `exclude` regex |
| 4 | `languageHandler` | Complex: rejects if unknown languages (when `removeUnknownLanguages`), missing required languages, or contains excluded languages. Allows English bypass via `allowEnglishInLanguages`. Supports language group expansion (`anime`, `nonAnime`, `common`) |
| 5 | `fetchResolution` | Resolution not enabled in settings |
| 6 | `checkFetchMap(quality)` | Quality's custom rank has `fetch: false` |
| 7 | `checkFetchList(audio/hdr)` | Any audio/HDR value has `fetch: false` |
| 8 | `checkFetchFlags` | Any boolean flag field has `fetch: false` |

**Override**: Even if checks fail, if `rawTitle` matches any `compiled.require` pattern, `fetch` is forced to `true`.

---

## 5. Title Normalisation & Similarity

### Normalisation (`lib/shared/normalise.ts`)

1. Lowercase (optional).
2. NFKC Unicode normalisation.
3. Character-by-character transliteration via a 90-entry table (diacritics → ASCII, `&` → `and`, `_` → space, punctuation stripped).
4. Remove remaining non-alphanumeric/non-space characters.
5. Collapse whitespace.

### Levenshtein Similarity (`lib/ranker/lev.ts`)

- **Library**: `fastest-levenshtein`.
- **Formula**: `(len(a) + len(b) - distance) / (len(a) + len(b))` — matches Python's `python-Levenshtein.ratio()`.
- **Alias support**: Checks correct title + all alias values, returns the highest ratio.
- Returns `0` if best ratio < threshold (default `0.85`).

---

## 6. Settings & Ranking Model (`lib/ranker/settings.ts`)

### Settings Schema (Zod-validated, 649 lines)

```
Settings {
  require: string[]           // Regex patterns that override fetch failures
  exclude: string[]           // Regex patterns to reject
  preferred: string[]         // Regex patterns that give +10,000 boost
  resolutions: {              // Toggle which resolutions to accept
    r2160p: false, r1080p: true, r720p: true,
    r480p: false, r360p: false, unknown: true
  }
  options: {
    removeAllTrash: true,     // Master switch for all rejection logic
    removeRanksUnder: -10000, // Floor for total score
    removeUnknownLanguages: false,
    allowEnglishInLanguages: true,
    removeAdultContent: true,
    titleSimilarity: 0.85     // Levenshtein threshold
  }
  languages: { required[], allowed[], exclude[], preferred[] }
  customRanks: {              // 6 categories, each with fetch+rank overrides
    quality: { av1, avc, bluray, dvd, hdtv, hevc, mpeg, remux, vhs, web, webdl, webmux, xvid }
    rips: { bdrip, brrip, dvdrip, hdrip, ppvrip, satrip, tvrip, uhdrip, vhsrip, webdlrip, webrip }
    hdr: { bit10, dolbyVision, hdr, hdr10plus, sdr }
    audio: { aac, atmos, dolbyDigital, dolbyDigitalPlus, dtsLossy, dtsLossless, flac, mono, mp3, stereo, surround, truehd }
    extras: { threeD, converted, documentary, dubbed, edition, hardcoded, network, proper, repack, retail, site, subbed, upscaled, scene, uncensored }
    trash: { cam, cleanAudio, pdtv, r5, screener, size, telecine, telesync }
  }
}
```

### Default Ranking Model (key values)

| Tier | Examples | Score |
|---|---|---|
| **Premium** | remux | +10,000 |
| **Great** | dolbyVision +3,000 · hdr10plus +2,100 · hdr +2,000 · truehd +2,000 · dtsLossless +2,000 |
| **Good** | atmos +1,000 · avc/hevc/av1 +500 · bluray +100 · webdl +200 |
| **Neutral** | Many extras at 0 · sdr omitted (defaults to 0) |
| **Bad** | webrip −1,000 · mpeg −1,000 · converted −1,000 · dubbed −1,000 |
| **Terrible** | hdtv/dvd −5,000 · bdrip/dvdrip −5,000 |
| **Banned** | cam/telesync/telecine/screener/vhs/webmux/xvid/site −10,000 |

---

## 7. Sorting & Bucketing (`lib/ranker/sort.ts`)

1. **Resolution filter** — Drops torrents whose resolution isn't in the enabled set.
2. **Primary sort** — Descending by `rank` (total score).
3. **Bucket limit** (optional) — Caps results per resolution bucket. E.g., `bucketLimit=2` keeps at most 2 results per resolution tier, ensuring variety.

**Secondary tiebreak** (in the `riven` app integration, `sort-by-rank-and-resolution.ts`):
- When `rank` is equal, higher `ResolutionRank` wins (2160p > 1080p > 720p > …).

---

## 8. Error Hierarchy (`lib/ranker/exceptions.ts`)

```
GarbageTorrentError (base)
├── TitleSimilarityError      — parsed title doesn't match correct title
├── InvalidHashError          — hash fails SHA-1 validation
├── FetchChecksFailedError    — one or more fetch checks failed
└── RankUnderThresholdError   — total score below removeRanksUnder
```

All extend `Error` and are caught gracefully in the pipeline (logged at `silly` level, torrent skipped).

---

## 9. Integration in riven App

RTN is used at **two distinct stages** in the BullMQ message-queue flow:

### Stage 1 — Scrape Pipeline (`flows/scrape-item/`)

| Step | File | RTN Usage |
|---|---|---|
| **Collect raw provider results** | scrape worker / provider clients | Queries the configured scraper plugins, persists raw candidates to PostgreSQL, and only then hands off to RTN parsing. In current upstream `main`, the verified app dependency graph includes `plugin-torrentio` and `plugin-comet` on this side of the pipeline. |
| **Parse scrape results** | `parse-scrape-results.processor.ts` | Calls `parse(rawTitle)` on every scraper result to get `ParsedData` |
| **Validate torrent** | `validate-torrent.ts` | Uses `ParsedData` fields (country, year, seasons, episodes) to reject torrents that don't match the requested media item. Separate from RTN's fetch checks — this is **content-level** validation (wrong season, wrong movie, etc.) |

### Stage 2 — Download Pipeline (`flows/download-item/`)

| Step | File | RTN Usage |
|---|---|---|
| **Enqueue** | `enqueue-download-item.ts` | Creates a concrete `RTN Settings` + `RankingModel` (with customised scores different from defaults, e.g. `webdl: 1500`, `bluray: 500`, `sdr: 2300`) and passes them into the flow |
| **Rank streams** | `rank-streams.processor.ts` | Instantiates `new RTN(settings, model)`, calls `rtnInstance.rankTorrent()` per stream, then `rtnInstance.sortTorrents()` for bucket-limited sorting |
| **Sort tiebreak** | `sort-by-rank-and-resolution.ts` | Final `.sort()` using `ResolutionRank` as tiebreaker when scores are equal |
| **Anime filter** | Inside rank-streams processor | Skips non-dubbed anime if `settings.dubbedAnimeOnly` is enabled |

### Settings vs Defaults in Production

The `enqueue-download-item.ts` file overrides many defaults to be **more permissive** in fetching (e.g., `av1.fetch: true`, `remux.fetch: true`, `bdrip/dvdrip.fetch: true`, `site.fetch: true`) and uses a **different scoring model** that values WEB-DL (1,500), BDRip (1,000), and SDR (2,300) much higher than the package defaults.

---

## 10. Key Design Observations

1. **Two-phase separation**: Raw provider collection, parsing/content validation (scrape-time), and ranking/fetch-checking (download-time) remain decoupled. Raw scrape candidates are now persisted first, then parsed into durable stream rows so later stages can reuse them without re-fetching or re-parsing queue payloads.
2. **Dual-axis filtering**: `fetch` (boolean per attribute) controls whether a torrent is *allowed*, while `rank` (numeric per attribute) controls *preference*. This is more granular than a single score threshold.
3. **Required override**: The `require` patterns act as a whitelist that overrides all fetch failures — a safety valve for edge cases.
4. **Preferred boost is binary**: Either +10,000 or +0. No gradient. This effectively makes preferred patterns/languages always win over non-preferred regardless of other quality differences.
5. **No caching**: The `RTN` class is stateless per-call. No result caching between invocations.
6. **Schema-driven validation**: Every input/output boundary uses Zod schemas, giving automatic validation and type inference throughout.
