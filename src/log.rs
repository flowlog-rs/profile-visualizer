//! Parsing for the Timely time log and memory log.
//!
//! Supports reading a folder of per-worker log files (e.g. time_worker_0.log,
//! time_worker_1.log, ...) and aggregating into mean + variance across workers.

use crate::Result;
use crate::addr::Addr;
use crate::diagnostics;
use crate::stats::Stats;

use anyhow::{Context, anyhow, bail};
use regex::Regex;
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::Path;
use std::sync::LazyLock;

// ---------------------------------------------------------------------------
// Aggregated row types (mean + variance across workers)
// ---------------------------------------------------------------------------

/// Aggregated time row across workers.
#[derive(Debug, Clone)]
pub struct TimeRow {
    pub activations: Stats,
    pub total_active_ms: Stats,
    pub op_name: String,
    pub num_workers: usize,
}

pub type TimeIndex = BTreeMap<Addr, TimeRow>;

/// Aggregated memory row across workers.
#[derive(Debug, Clone)]
pub struct MemoryRow {
    pub batched_in: Stats,
    pub merges: Stats,
    pub merge_in: Stats,
    pub merge_out: Stats,
    pub dropped: Stats,
    pub op_name: String,
    pub num_workers: usize,
}

pub type MemoryIndex = BTreeMap<Addr, MemoryRow>;

// ---------------------------------------------------------------------------
// Raw per-worker row types (internal)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct RawTimeRow {
    activations: u64,
    total_active_ms: f64,
    op_name: String,
}

type RawTimeIndex = BTreeMap<Addr, RawTimeRow>;

#[derive(Debug, Clone)]
struct RawMemoryRow {
    batched_in: u64,
    merges: u64,
    merge_in: u64,
    merge_out: u64,
    dropped: u64,
    op_name: String,
}

type RawMemoryIndex = BTreeMap<Addr, RawMemoryRow>;

// ---------------------------------------------------------------------------
// Public API: folder-based parsing
// ---------------------------------------------------------------------------

/// Parse all time log files in a folder (*.log) and aggregate into mean + variance.
pub fn parse_time_folder(dir: &str) -> Result<TimeIndex> {
    let files = collect_log_files(dir)?;
    if files.is_empty() {
        bail!(
            "{}",
            diagnostics::error_message(format!("no .log files found in time folder {}", dir))
        );
    }

    let mut all: Vec<RawTimeIndex> = Vec::new();
    for f in &files {
        all.push(parse_raw_time_file(f)?);
    }

    aggregate_time(&all, &files)
}

/// Parse all memory log files in a folder (*.log) and aggregate into mean + variance.
pub fn parse_memory_folder(dir: &str) -> Result<MemoryIndex> {
    let files = collect_log_files(dir)?;
    if files.is_empty() {
        bail!(
            "{}",
            diagnostics::error_message(format!(
                "no .log files found in memory folder {}",
                dir
            ))
        );
    }

    let mut all: Vec<RawMemoryIndex> = Vec::new();
    for f in &files {
        all.push(parse_raw_memory_file(f)?);
    }

    aggregate_memory(&all, &files)
}

// ---------------------------------------------------------------------------
// Aggregation helpers
// ---------------------------------------------------------------------------

/// Validate that op_name is consistent across workers for a given addr.
fn validate_op_name(
    op_name: &mut Option<String>,
    candidate: &str,
    addr: &Addr,
    first_file: &str,
    current_file: &str,
) -> Result<()> {
    if let Some(existing) = op_name.as_ref() {
        if existing != candidate {
            bail!(
                "{}",
                diagnostics::error_message(format!(
                    "op_name mismatch for addr {:?} between {} ({:?}) and {} ({:?})",
                    addr.0, first_file, existing, current_file, candidate
                ))
            );
        }
    } else {
        *op_name = Some(candidate.to_string());
    }
    Ok(())
}

fn aggregate_time(workers: &[RawTimeIndex], files: &[String]) -> Result<TimeIndex> {
    let n = workers.len();
    let all_addrs: BTreeSet<&Addr> = workers.iter().flat_map(|w| w.keys()).collect();

    let mut out = TimeIndex::new();
    for addr in all_addrs {
        let mut activations = Vec::with_capacity(n);
        let mut ms = Vec::with_capacity(n);
        let mut op_name: Option<String> = None;

        for (wi, w) in workers.iter().enumerate() {
            match w.get(addr) {
                Some(row) => {
                    validate_op_name(&mut op_name, &row.op_name, addr, &files[0], &files[wi])?;
                    activations.push(row.activations as f64);
                    ms.push(row.total_active_ms);
                }
                None => {
                    activations.push(0.0);
                    ms.push(0.0);
                }
            }
        }

        out.insert(
            addr.clone(),
            TimeRow {
                activations: Stats::from_values(&activations),
                total_active_ms: Stats::from_values(&ms),
                op_name: op_name.unwrap_or_default(),
                num_workers: n,
            },
        );
    }

    Ok(out)
}

fn aggregate_memory(workers: &[RawMemoryIndex], files: &[String]) -> Result<MemoryIndex> {
    let n = workers.len();
    let all_addrs: BTreeSet<&Addr> = workers.iter().flat_map(|w| w.keys()).collect();

    let mut out = MemoryIndex::new();
    for addr in all_addrs {
        let mut batched_in = Vec::with_capacity(n);
        let mut merges = Vec::with_capacity(n);
        let mut merge_in = Vec::with_capacity(n);
        let mut merge_out = Vec::with_capacity(n);
        let mut dropped = Vec::with_capacity(n);
        let mut op_name: Option<String> = None;

        for (wi, w) in workers.iter().enumerate() {
            match w.get(addr) {
                Some(row) => {
                    validate_op_name(&mut op_name, &row.op_name, addr, &files[0], &files[wi])?;
                    batched_in.push(row.batched_in as f64);
                    merges.push(row.merges as f64);
                    merge_in.push(row.merge_in as f64);
                    merge_out.push(row.merge_out as f64);
                    dropped.push(row.dropped as f64);
                }
                None => {
                    batched_in.push(0.0);
                    merges.push(0.0);
                    merge_in.push(0.0);
                    merge_out.push(0.0);
                    dropped.push(0.0);
                }
            }
        }

        out.insert(
            addr.clone(),
            MemoryRow {
                batched_in: Stats::from_values(&batched_in),
                merges: Stats::from_values(&merges),
                merge_in: Stats::from_values(&merge_in),
                merge_out: Stats::from_values(&merge_out),
                dropped: Stats::from_values(&dropped),
                op_name: op_name.unwrap_or_default(),
                num_workers: n,
            },
        );
    }

    Ok(out)
}

// ---------------------------------------------------------------------------
// File collection
// ---------------------------------------------------------------------------

fn collect_log_files(dir: &str) -> Result<Vec<String>> {
    let path = Path::new(dir);
    if !path.is_dir() {
        bail!(
            "{}",
            diagnostics::error_message(format!("{} is not a directory", dir))
        );
    }

    let mut files: Vec<String> = Vec::new();
    for entry in fs::read_dir(path)
        .with_context(|| diagnostics::error_message(format!("read directory {}", dir)))?
    {
        let entry = entry?;
        let p = entry.path();
        if p.is_file() {
            if let Some(ext) = p.extension() {
                if ext == "log" {
                    files.push(p.to_string_lossy().to_string());
                }
            }
        }
    }
    files.sort();
    Ok(files)
}

// ---------------------------------------------------------------------------
// Compiled regexes (compiled once)
// ---------------------------------------------------------------------------

static TIME_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r#"^\s*(\[[^\]]*\])\s+(\d+)\s+([0-9]+(?:\.[0-9]+)?)\s+(.*?)\s*$"#).unwrap()
});

static MEMORY_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r#"^\s*(\[[^\]]*\])\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(.*?)\s*$"#)
        .unwrap()
});

// ---------------------------------------------------------------------------
// Raw single-file parsers (internal)
// ---------------------------------------------------------------------------

fn parse_raw_time_file(path: &str) -> Result<RawTimeIndex> {
    let text = fs::read_to_string(path)
        .with_context(|| diagnostics::error_message(format!("read time log file {}", path)))?;

    let re = &*TIME_RE;

    let mut out = RawTimeIndex::new();
    for (lineno, line) in text.lines().enumerate() {
        let lno = lineno + 1;
        let line = line.trim_end();

        if line.trim().is_empty() {
            continue;
        }

        if line.contains("addr") && line.contains("activations") && line.contains("total_active_ms")
        {
            continue;
        }

        let caps = match re.captures(line) {
            Some(c) => c,
            None => {
                bail!(
                    "{}",
                    diagnostics::error_message(format!(
                        "time log parse error at {}:{}: cannot parse line: {:?}",
                        path, lno, line
                    ))
                );
            }
        };

        let addr_str = caps
            .get(1)
            .ok_or_else(|| {
                anyhow!(diagnostics::error_message(format!(
                    "time log parse error at {}:{}: missing addr",
                    path, lno
                )))
            })?
            .as_str();
        let activations: u64 = caps
            .get(2)
            .ok_or_else(|| {
                anyhow!(diagnostics::error_message(format!(
                    "time log parse error at {}:{}: missing activations",
                    path, lno
                )))
            })?
            .as_str()
            .parse()?;
        let total_active_ms: f64 = caps
            .get(3)
            .ok_or_else(|| {
                anyhow!(diagnostics::error_message(format!(
                    "time log parse error at {}:{}: missing total_active_ms",
                    path, lno
                )))
            })?
            .as_str()
            .parse()?;
        let op_name = caps
            .get(4)
            .ok_or_else(|| {
                anyhow!(diagnostics::error_message(format!(
                    "time log parse error at {}:{}: missing name",
                    path, lno
                )))
            })?
            .as_str()
            .to_string();

        let addr = parse_addr(addr_str).with_context(|| {
            diagnostics::error_message(format!("bad addr at {}:{}: {}", path, lno, addr_str))
        })?;

        let row = RawTimeRow {
            activations,
            total_active_ms,
            op_name,
        };

        if out.insert(addr.clone(), row).is_some() {
            bail!(
                "{}",
                diagnostics::error_message(format!(
                    "duplicate addr entry in time log at {}:{}: {}",
                    path, lno, addr_str
                ))
            );
        }
    }

    Ok(out)
}

fn parse_raw_memory_file(path: &str) -> Result<RawMemoryIndex> {
    let text = fs::read_to_string(path)
        .with_context(|| diagnostics::error_message(format!("read memory file {}", path)))?;

    let re = &*MEMORY_RE;

    let mut out = RawMemoryIndex::new();
    for (lineno, line) in text.lines().enumerate() {
        let lno = lineno + 1;
        let line = line.trim_end();

        if line.trim().is_empty() {
            continue;
        }

        if line.contains("addr") && line.contains("batched_in") {
            continue;
        }

        let caps = match re.captures(line) {
            Some(c) => c,
            None => {
                bail!(
                    "{}",
                    diagnostics::error_message(format!(
                        "memory parse error at {}:{}: cannot parse line: {:?}",
                        path, lno, line
                    ))
                );
            }
        };

        macro_rules! get_u64 {
            ($idx:expr, $field:literal) => {
                caps.get($idx)
                    .ok_or_else(|| {
                        anyhow!(diagnostics::error_message(format!(
                            "memory parse error at {}:{}: missing {}",
                            path, lno, $field
                        )))
                    })?
                    .as_str()
                    .parse::<u64>()?
            };
        }

        let addr_str = caps.get(1).unwrap().as_str();
        let batched_in = get_u64!(2, "batched_in");
        let merges = get_u64!(3, "merges");
        let merge_in = get_u64!(4, "merge_in");
        let merge_out = get_u64!(5, "merge_out");
        let dropped = get_u64!(6, "dropped");
        let op_name = caps.get(7).unwrap().as_str().to_string();

        let addr = parse_addr(addr_str).with_context(|| {
            diagnostics::error_message(format!("bad addr at {}:{}: {}", path, lno, addr_str))
        })?;

        let row = RawMemoryRow {
            batched_in,
            merges,
            merge_in,
            merge_out,
            dropped,
            op_name,
        };

        if out.insert(addr.clone(), row).is_some() {
            bail!(
                "{}",
                diagnostics::error_message(format!(
                    "duplicate addr entry in memory log at {}:{}: {}",
                    path, lno, addr_str
                ))
            );
        }
    }

    Ok(out)
}

/// Parse "[0, 8, 10]" into Addr(vec![0, 8, 10]).
fn parse_addr(s: &str) -> Result<Addr> {
    let inner = s
        .trim()
        .strip_prefix('[')
        .and_then(|s| s.strip_suffix(']'))
        .ok_or_else(|| {
            anyhow!(diagnostics::error_message(format!(
                "addr must be bracketed: {}",
                s
            )))
        })?
        .trim();
    if inner.is_empty() {
        return Ok(Addr::new(vec![]));
    }
    let mut v = Vec::new();
    for part in inner.split(',') {
        let p = part.trim();
        if p.is_empty() {
            continue;
        }
        v.push(
            p.parse::<u32>()
                .with_context(|| diagnostics::error_message(format!("bad addr element {}", p)))?,
        );
    }
    Ok(Addr::new(v))
}
