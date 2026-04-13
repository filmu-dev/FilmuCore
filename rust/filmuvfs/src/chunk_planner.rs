use std::cmp;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadPattern {
    HeaderScan,
    SequentialScan,
    RandomAccess,
    TailProbe,
    CacheHit,
}

impl ReadPattern {
    #[must_use]
    pub fn should_prefetch(self) -> bool {
        matches!(self, Self::SequentialScan)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PlannedChunk {
    pub offset: u64,
    pub length: u64,
}

impl PlannedChunk {
    #[must_use]
    pub fn end_inclusive(self) -> u64 {
        self.offset.saturating_add(self.length.saturating_sub(1))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlannedRead {
    pub pattern: ReadPattern,
    pub chunks: Vec<PlannedChunk>,
    pub prefetch_chunks: Vec<PlannedChunk>,
    pub request_end_exclusive: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FileGeometry {
    pub file_size: u64,
    pub header_size: u64,
    pub header_end: u64,
    pub footer_size: u64,
    pub footer_start: u64,
}

#[derive(Debug, Clone)]
pub struct ChunkPlannerConfig {
    pub header_size: u64,
    pub min_footer_size: u64,
    pub max_footer_size: u64,
    pub target_footer_pct: f64,
    pub block_size: u64,
    pub scan_tolerance_bytes: u64,
    pub sequential_read_tolerance_blocks: u64,
    pub scan_chunk_size: u64,
    pub random_chunk_size: u64,
    pub sequential_prefetch_chunks: usize,
}

impl Default for ChunkPlannerConfig {
    fn default() -> Self {
        Self {
            header_size: 131_072,
            min_footer_size: 131_072,
            max_footer_size: 2_097_152,
            target_footer_pct: 0.02,
            block_size: 4_096,
            scan_tolerance_bytes: 524_288,
            sequential_read_tolerance_blocks: 8,
            scan_chunk_size: 1_048_576,
            random_chunk_size: 262_144,
            sequential_prefetch_chunks: 2,
        }
    }
}

impl ChunkPlannerConfig {
    #[must_use]
    pub fn with_chunk_sizes(scan_chunk_size: u64, random_chunk_size: u64) -> Self {
        Self {
            scan_chunk_size,
            random_chunk_size,
            ..Self::default()
        }
    }
}

#[derive(Debug, Clone)]
pub struct ChunkPlanner {
    config: ChunkPlannerConfig,
}

impl Default for ChunkPlanner {
    fn default() -> Self {
        Self::new(ChunkPlannerConfig::default())
    }
}

impl ChunkPlanner {
    #[must_use]
    pub fn new(config: ChunkPlannerConfig) -> Self {
        Self { config }
    }

    #[must_use]
    pub fn config(&self) -> &ChunkPlannerConfig {
        &self.config
    }

    #[must_use]
    pub fn plan_read(
        &self,
        offset: u64,
        length: u32,
        file_size: Option<u64>,
        previous_end_exclusive: Option<u64>,
    ) -> PlannedRead {
        if length == 0 {
            return PlannedRead {
                pattern: ReadPattern::RandomAccess,
                chunks: Vec::new(),
                prefetch_chunks: Vec::new(),
                request_end_exclusive: offset,
            };
        }

        match file_size {
            Some(known_size) if offset >= known_size => PlannedRead {
                pattern: ReadPattern::RandomAccess,
                chunks: Vec::new(),
                prefetch_chunks: Vec::new(),
                request_end_exclusive: known_size,
            },
            Some(known_size) => {
                self.plan_known_size(offset, length, known_size, previous_end_exclusive)
            }
            None => self.plan_unknown_size(offset, length, previous_end_exclusive),
        }
    }

    #[must_use]
    pub fn geometry_for_file(&self, file_size: u64) -> FileGeometry {
        let header_size = cmp::min(self.config.header_size, file_size);
        let header_end = header_size.saturating_sub(1);

        if file_size <= header_size {
            return FileGeometry {
                file_size,
                header_size,
                header_end,
                footer_size: 0,
                footer_start: file_size,
            };
        }

        let percentage_size = (file_size as f64 * self.config.target_footer_pct) as u64;
        let raw_footer_size = cmp::min(
            cmp::max(percentage_size, self.config.min_footer_size),
            self.config.max_footer_size,
        );
        let aligned_footer_size =
            (raw_footer_size / self.config.block_size) * self.config.block_size;
        let footer_size = aligned_footer_size.max(self.config.block_size);
        let footer_start = cmp::max(header_size, file_size.saturating_sub(footer_size));

        FileGeometry {
            file_size,
            header_size,
            header_end,
            footer_size: file_size.saturating_sub(footer_start),
            footer_start,
        }
    }

    fn plan_known_size(
        &self,
        offset: u64,
        length: u32,
        file_size: u64,
        previous_end_exclusive: Option<u64>,
    ) -> PlannedRead {
        let geometry = self.geometry_for_file(file_size);
        let request_end_exclusive = cmp::min(offset.saturating_add(u64::from(length)), file_size);
        let pattern = self.classify_known_size(
            offset,
            request_end_exclusive,
            geometry,
            previous_end_exclusive,
        );

        let mut chunks = Vec::new();
        if request_end_exclusive == 0 {
            return PlannedRead {
                pattern,
                chunks,
                prefetch_chunks: Vec::new(),
                request_end_exclusive,
            };
        }

        if request_end_exclusive.saturating_sub(1) <= geometry.header_end {
            chunks.push(PlannedChunk {
                offset: 0,
                length: geometry.header_size,
            });
        } else if offset >= geometry.footer_start {
            chunks.push(PlannedChunk {
                offset: geometry.footer_start,
                length: geometry.footer_size,
            });
        } else {
            if offset <= geometry.header_end && geometry.header_size > 0 {
                chunks.push(PlannedChunk {
                    offset: 0,
                    length: geometry.header_size,
                });
            }

            let body_floor = geometry.header_size;
            let body_ceiling = geometry.footer_start;
            if body_floor < body_ceiling {
                let body_start = cmp::max(offset, body_floor);
                let body_end_exclusive = cmp::min(request_end_exclusive, body_ceiling);
                if body_start < body_end_exclusive {
                    let chunk_size = self.chunk_size_for_pattern(pattern);
                    let mut current = align_down_from_origin(body_start, body_floor, chunk_size);
                    while current < body_end_exclusive {
                        let next_end = cmp::min(current.saturating_add(chunk_size), body_ceiling);
                        chunks.push(PlannedChunk {
                            offset: current,
                            length: next_end.saturating_sub(current),
                        });
                        current = next_end;
                    }
                }
            }

            if request_end_exclusive > geometry.footer_start && geometry.footer_size > 0 {
                chunks.push(PlannedChunk {
                    offset: geometry.footer_start,
                    length: geometry.footer_size,
                });
            }
        }

        let prefetch_chunks = self.plan_prefetch_chunks(pattern, &chunks, geometry);

        PlannedRead {
            pattern,
            chunks,
            prefetch_chunks,
            request_end_exclusive,
        }
    }

    fn plan_unknown_size(
        &self,
        offset: u64,
        length: u32,
        previous_end_exclusive: Option<u64>,
    ) -> PlannedRead {
        let request_end_exclusive = offset.saturating_add(u64::from(length));
        let pattern =
            self.classify_unknown_size(offset, request_end_exclusive, previous_end_exclusive);
        let chunk_size = self.chunk_size_for_pattern(pattern);
        let mut chunks = Vec::new();
        let mut current = align_down(offset, chunk_size);
        while current < request_end_exclusive {
            let next_end = cmp::min(current.saturating_add(chunk_size), request_end_exclusive);
            chunks.push(PlannedChunk {
                offset: current,
                length: next_end.saturating_sub(current),
            });
            current = next_end;
        }

        PlannedRead {
            pattern,
            chunks,
            prefetch_chunks: Vec::new(),
            request_end_exclusive,
        }
    }

    fn classify_known_size(
        &self,
        offset: u64,
        request_end_exclusive: u64,
        geometry: FileGeometry,
        previous_end_exclusive: Option<u64>,
    ) -> ReadPattern {
        if request_end_exclusive > 0
            && request_end_exclusive.saturating_sub(1) <= geometry.header_end
        {
            return ReadPattern::HeaderScan;
        }

        if offset >= geometry.footer_start {
            return ReadPattern::TailProbe;
        }

        if let Some(previous_end) = previous_end_exclusive {
            let tolerance = self
                .config
                .sequential_read_tolerance_blocks
                .saturating_mul(self.config.block_size);
            if offset >= previous_end && offset.saturating_sub(previous_end) <= tolerance {
                return ReadPattern::SequentialScan;
            }
        }

        if offset < 10 * 1024 * 1024
            && request_end_exclusive.saturating_sub(offset) >= self.config.scan_chunk_size / 2
        {
            return ReadPattern::SequentialScan;
        }

        ReadPattern::RandomAccess
    }

    fn classify_unknown_size(
        &self,
        offset: u64,
        request_end_exclusive: u64,
        previous_end_exclusive: Option<u64>,
    ) -> ReadPattern {
        if let Some(previous_end) = previous_end_exclusive {
            let tolerance = self
                .config
                .sequential_read_tolerance_blocks
                .saturating_mul(self.config.block_size);
            if offset >= previous_end && offset.saturating_sub(previous_end) <= tolerance {
                return ReadPattern::SequentialScan;
            }
        }

        if offset < 10 * 1024 * 1024
            && request_end_exclusive.saturating_sub(offset) >= self.config.scan_chunk_size / 2
        {
            return ReadPattern::SequentialScan;
        }

        ReadPattern::RandomAccess
    }

    fn chunk_size_for_pattern(&self, pattern: ReadPattern) -> u64 {
        match pattern {
            ReadPattern::HeaderScan | ReadPattern::SequentialScan => self.config.scan_chunk_size,
            ReadPattern::RandomAccess | ReadPattern::TailProbe | ReadPattern::CacheHit => {
                self.config.random_chunk_size
            }
        }
    }

    fn plan_prefetch_chunks(
        &self,
        pattern: ReadPattern,
        chunks: &[PlannedChunk],
        geometry: FileGeometry,
    ) -> Vec<PlannedChunk> {
        if !pattern.should_prefetch() || chunks.is_empty() {
            return Vec::new();
        }

        let body_floor = geometry.header_size;
        let body_ceiling = geometry.footer_start;
        if body_floor >= body_ceiling {
            return Vec::new();
        }

        let chunk_size = self.chunk_size_for_pattern(pattern);
        let mut next_offset = chunks
            .iter()
            .filter(|chunk| chunk.offset >= body_floor && chunk.offset < body_ceiling)
            .map(|chunk| chunk.offset.saturating_add(chunk.length))
            .max()
            .unwrap_or(body_floor);
        let mut planned = Vec::new();
        while planned.len() < self.config.sequential_prefetch_chunks && next_offset < body_ceiling {
            let aligned_offset = align_down_from_origin(next_offset, body_floor, chunk_size);
            let chunk_end = cmp::min(aligned_offset.saturating_add(chunk_size), body_ceiling);
            let chunk = PlannedChunk {
                offset: aligned_offset,
                length: chunk_end.saturating_sub(aligned_offset),
            };
            if !chunks.contains(&chunk) && !planned.contains(&chunk) {
                planned.push(chunk);
            }
            next_offset = chunk_end;
        }
        planned
    }
}

fn align_down(value: u64, alignment: u64) -> u64 {
    if alignment == 0 {
        return value;
    }
    (value / alignment) * alignment
}

fn align_down_from_origin(value: u64, origin: u64, alignment: u64) -> u64 {
    if alignment == 0 || value <= origin {
        return origin;
    }
    origin.saturating_add(align_down(value.saturating_sub(origin), alignment))
}

#[cfg(test)]
mod tests {
    use super::{ChunkPlanner, ReadPattern};

    fn assert_chunks_cover_request(
        chunks: &[super::PlannedChunk],
        request_offset: u64,
        request_end_exclusive: u64,
    ) {
        assert!(!chunks.is_empty(), "expected at least one planned chunk");
        assert!(
            chunks[0].offset <= request_offset,
            "first chunk must start before request start"
        );
        assert!(
            chunks
                .last()
                .map(|chunk| chunk.end_inclusive() + 1 >= request_end_exclusive)
                .unwrap_or(false),
            "last chunk must cover request end"
        );
        for window in chunks.windows(2) {
            let left = window[0];
            let right = window[1];
            assert!(
                right.offset >= left.offset,
                "chunks must stay sorted by offset"
            );
            assert!(
                right.offset <= left.end_inclusive().saturating_add(1),
                "chunks must be contiguous or overlapping"
            );
        }
    }

    #[test]
    fn chunk_parity_contract_header_only() {
        let planner = ChunkPlanner::default();
        let file_size = 1_073_741_824;
        let planned = planner.plan_read(0, 4_096, Some(file_size), None);
        assert_eq!(planned.pattern, ReadPattern::HeaderScan);
        assert_eq!(planned.chunks.len(), 1);
        assert_eq!(planned.chunks[0].offset, 0);
        assert_chunks_cover_request(&planned.chunks, 0, planned.request_end_exclusive);
    }

    #[test]
    fn chunk_parity_contract_footer_only() {
        let planner = ChunkPlanner::default();
        let file_size = 1_073_741_824;
        let geometry = planner.geometry_for_file(file_size);
        let planned = planner.plan_read(geometry.footer_start, 4_096, Some(file_size), None);
        assert_eq!(planned.pattern, ReadPattern::TailProbe);
        assert_eq!(planned.chunks.len(), 1);
        assert_eq!(planned.chunks[0].offset, geometry.footer_start);
        assert_chunks_cover_request(
            &planned.chunks,
            geometry.footer_start,
            planned.request_end_exclusive,
        );
    }

    #[test]
    fn chunk_parity_contract_body_random_access() {
        let planner = ChunkPlanner::default();
        let file_size = 1_073_741_824;
        let offset = 16 * 1024 * 1024;
        let planned = planner.plan_read(offset, 1_024, Some(file_size), None);
        assert_eq!(planned.pattern, ReadPattern::RandomAccess);
        assert_eq!(planned.chunks.len(), 1);
        assert_chunks_cover_request(&planned.chunks, offset, planned.request_end_exclusive);
    }

    #[test]
    fn chunk_parity_contract_header_body_boundary() {
        let planner = ChunkPlanner::default();
        let file_size = 1_073_741_824;
        let geometry = planner.geometry_for_file(file_size);
        let offset = geometry.header_end.saturating_sub(1_024);
        let planned = planner.plan_read(offset, 4_096, Some(file_size), None);
        assert!(
            matches!(
                planned.pattern,
                ReadPattern::RandomAccess | ReadPattern::SequentialScan
            ),
            "expected non-tail access classification at header boundary"
        );
        assert!(
            planned.chunks.len() >= 2,
            "expected header+body chunks at header boundary"
        );
        assert_eq!(planned.chunks[0].offset, 0);
        assert_chunks_cover_request(&planned.chunks, offset, planned.request_end_exclusive);
    }

    #[test]
    fn chunk_parity_contract_body_footer_boundary() {
        let planner = ChunkPlanner::default();
        let file_size = 1_073_741_824;
        let geometry = planner.geometry_for_file(file_size);
        let offset = geometry.footer_start.saturating_sub(1_024);
        let planned = planner.plan_read(offset, 4_096, Some(file_size), None);
        assert!(
            matches!(
                planned.pattern,
                ReadPattern::RandomAccess | ReadPattern::SequentialScan
            ),
            "expected non-tail body access classification at boundary"
        );
        assert!(
            planned.chunks.len() >= 2,
            "expected body+footer chunks at footer boundary"
        );
        let last = planned
            .chunks
            .last()
            .copied()
            .expect("expected footer chunk at boundary");
        assert_eq!(last.offset, geometry.footer_start);
        assert_chunks_cover_request(&planned.chunks, offset, planned.request_end_exclusive);
    }
}
