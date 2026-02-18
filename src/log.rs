//! Parsing for the Timely time log (time.tsv) and memory log (memory.tsv).

use crate::Result;
use crate::addr::Addr;
use crate::diagnostics;

use anyhow::{Context, anyhow, bail};
use regex::Regex;
use std::collections::BTreeMap;
use std::fs;

/// A single operator row from the Timely time log.
#[derive(Debug, Clone)]
pub struct TimeRow {
    pub activations: u64,
    pub total_active_ms: f64,
    pub op_name: String,
}

/// Index by address for fast lookup during aggregation.
pub type TimeIndex = BTreeMap<Addr, TimeRow>;

/// A single operator row from the memory profile table.
#[derive(Debug, Clone)]
pub struct MemoryRow {
    pub batched_in: u64,
    pub merges: u64,
    pub merge_in: u64,
    pub merge_out: u64,
    pub dropped: u64,
    pub op_name: String,
}

/// Index by address for fast lookup during memory aggregation.
pub type MemoryIndex = BTreeMap<Addr, MemoryRow>;

/// Parse the Timely time log (time.tsv) into an address-to-row index.
///
/// Expected columns (whitespace-separated):
/// addr  activations  total_active_ms  name...
///
/// Example:
/// [0, 8, 10]   33   853.886   ThresholdTotal
pub fn parse_time_file(path: &str) -> Result<TimeIndex> {
    let text = fs::read_to_string(path)
        .with_context(|| diagnostics::error_message(format!("read time log file {}", path)))?;

    // We allow variable spacing; name may contain spaces and ':'.
    // Capture:
    // 1) addr: \[ ... \]
    // 2) activations: integer
    // 3) total_active_ms: float/integer
    // 4) name: rest of line
    const LOG_LINE_RE: &str = r#"^\s*(\[[^\]]*\])\s+(\d+)\s+([0-9]+(?:\.[0-9]+)?)\s+(.*?)\s*$"#;
    let re = Regex::new(LOG_LINE_RE)?;

    let mut out: TimeIndex = TimeIndex::new();
    for (lineno, line) in text.lines().enumerate() {
        let lno = lineno + 1;
        let line = line.trim_end();

        if line.trim().is_empty() {
            continue;
        }

        // Skip header line if present.
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

        let row = TimeRow {
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

/// Parse a memory profile table log file into an address-to-row index.
///
/// Expected columns (whitespace-separated):
/// addr  batched_in  merges  merge_in  merge_out  dropped  name...
///
/// Example:
/// [0, 11, 9]   8082820   7   11644270   7853107   4291657   Arrange: ThresholdTotal
pub fn parse_memory_file(path: &str) -> Result<MemoryIndex> {
    let text = fs::read_to_string(path)
        .with_context(|| diagnostics::error_message(format!("read memory file {}", path)))?;

    // Capture: addr, batched_in, merges, merge_in, merge_out, dropped, name
    const MEM_LINE_RE: &str =
        r#"^\s*(\[[^\]]*\])\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(.*?)\s*$"#;
    let re = Regex::new(MEM_LINE_RE)?;

    let mut out: MemoryIndex = MemoryIndex::new();
    for (lineno, line) in text.lines().enumerate() {
        let lno = lineno + 1;
        let line = line.trim_end();

        if line.trim().is_empty() {
            continue;
        }

        // Skip header line if present.
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

        let row = MemoryRow {
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
    let s = s.trim();
    if !s.starts_with('[') || !s.ends_with(']') {
        bail!(
            "{}",
            diagnostics::error_message(format!("addr must be bracketed: {}", s))
        );
    }
    let inner = &s[1..s.len() - 1].trim();
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
