use crate::log::row::{LogIndex, LogRow};
use crate::spec::Addr;
use anyhow::{Context, bail};
use regex::Regex;
use std::fs;

/// Parse a Timely profile table log file into an address->row index.
///
/// Expected columns (whitespace-separated):
/// addr  activations  total_active_ms  name...
///
/// Example:
/// [0, 8, 10]   33   853.886   ThresholdTotal
pub fn parse_log_file(path: &str) -> anyhow::Result<LogIndex> {
    let text = fs::read_to_string(path).with_context(|| format!("read log file {}", path))?;

    // We allow variable spacing; name may contain spaces and ':'.
    // Capture:
    // 1) addr: \[ ... \]
    // 2) activations: integer
    // 3) total_active_ms: float/integer
    // 4) name: rest of line
    let re = Regex::new(r#"^\s*(\[[^\]]*\])\s+(\d+)\s+([0-9]+(?:\.[0-9]+)?)\s+(.*?)\s*$"#)?;

    let mut out: LogIndex = LogIndex::new();
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
                    "log parse error at {}:{}: cannot parse line: {:?}",
                    path,
                    lno,
                    line
                );
            }
        };

        let addr_str = caps.get(1).unwrap().as_str();
        let activations: u64 = caps.get(2).unwrap().as_str().parse()?;
        let total_active_ms: f64 = caps.get(3).unwrap().as_str().parse()?;
        let op_name = caps.get(4).unwrap().as_str().to_string();

        let addr = parse_addr(addr_str)
            .with_context(|| format!("bad addr at {}:{}: {}", path, lno, addr_str))?;

        let row = LogRow {
            addr: addr.clone(),
            activations,
            total_active_ms,
            op_name,
        };

        if out.insert(addr.clone(), row).is_some() {
            bail!(
                "duplicate addr entry in log at {}:{}: {}",
                path,
                lno,
                addr_str
            );
        }
    }

    Ok(out)
}

/// Parse "[0, 8, 10]" into Addr(vec![0,8,10]).
fn parse_addr(s: &str) -> anyhow::Result<Addr> {
    let s = s.trim();
    if !s.starts_with('[') || !s.ends_with(']') {
        bail!("addr must be bracketed: {}", s);
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
                .with_context(|| format!("bad addr element {}", p))?,
        );
    }
    Ok(Addr::new(v))
}
