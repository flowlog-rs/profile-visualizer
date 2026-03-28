use anyhow::{Context, bail};
use clap::Parser;
use std::collections::BTreeMap;
use std::fs;

mod addr;
mod diagnostics;
mod log;
mod ops;
mod render;
mod stats;
mod view;

pub type Result<T> = anyhow::Result<T>;

#[derive(Parser)]
#[command(name = "flowlog-profile-viz")]
#[command(about = "FlowLog profile visualizer", long_about = None)]
struct Cli {
    /// Path to the ops.json spec.
    #[arg(short = 'p', long)]
    ops: String,

    /// Path to the folder containing time log files (*.log).
    #[arg(short = 't', long)]
    time: String,

    /// Path to the folder containing memory log files (*.log).
    #[arg(short = 'm', long)]
    memory: String,

    /// Output HTML file.
    #[arg(short = 'o', long)]
    out: String,
}

fn main() -> Result<()> {
    let Cli {
        ops,
        time,
        memory,
        out,
    } = Cli::parse();

    // 1) Parse + validate ops.json.
    let ops_text = fs::read_to_string(&ops)
        .with_context(|| diagnostics::error_message(format!("read ops file {}", ops)))?;
    let ops_spec: ops::OpsSpec = serde_json::from_str(&ops_text)
        .with_context(|| diagnostics::error_message(format!("parse ops file {}", ops)))?;
    let validated = ops_spec.validate_and_build()?;

    let ops::ValidatedOps {
        nodes,
        roots: root_ids,
        rules,
        fingerprint_to_node,
    } = validated;

    let mut nodes_by_name = BTreeMap::new();
    for (id, node) in nodes {
        nodes_by_name.insert(id.to_string(), node);
    }

    let roots: Vec<String> = root_ids.iter().map(|id| id.to_string()).collect();

    let fingerprint_to_node: BTreeMap<String, String> = fingerprint_to_node
        .into_iter()
        .map(|(fp, id)| (fp, id.to_string()))
        .collect();

    // 2) Parse time and memory log folders (auto-detects batch vs timestamped).
    let time_snapshots = log::parse_time_folder(&time)?;
    let memory_snapshots = log::parse_memory_folder(&memory)?;

    // Validate that time and memory have the same snapshot labels.
    let time_labels: Vec<&str> = time_snapshots.iter().map(|s| s.label.as_str()).collect();
    let mem_labels: Vec<&str> = memory_snapshots.iter().map(|s| s.label.as_str()).collect();
    if time_labels != mem_labels {
        bail!(
            "{}",
            diagnostics::error_message(format!(
                "time and memory folders have different snapshots: {:?} vs {:?}",
                time_labels, mem_labels
            ))
        );
    }

    // 3) Build one ReportData per snapshot.
    let mut snapshot_labels: Vec<String> = Vec::new();
    let mut snapshots: Vec<view::ReportData> = Vec::new();

    for (ts, ms) in time_snapshots.iter().zip(memory_snapshots.iter()) {
        snapshot_labels.push(ts.label.clone());
        snapshots.push(view::build_report_data(
            &nodes_by_name,
            &roots,
            &rules,
            &fingerprint_to_node,
            &ts.data,
            &ms.data,
        )?);
    }

    // 4) Render HTML.
    let html = render::render_html_report(&snapshot_labels, &snapshots)?;
    fs::write(&out, html)
        .with_context(|| diagnostics::error_message(format!("write output file {}", out)))?;
    println!("Wrote {} ({} snapshot(s): {})", out, snapshot_labels.len(), snapshot_labels.join(", "));

    Ok(())
}
