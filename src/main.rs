use anyhow::Context;
use clap::Parser;
use std::collections::BTreeMap;
use std::fs;

mod addr;
mod diagnostics;
mod log;
mod ops;
mod render;
mod view;

pub type Result<T> = anyhow::Result<T>;

#[derive(Parser)]
#[command(name = "flowlog-profile-viz")]
#[command(about = "FlowLog profile visualizer", long_about = None)]
struct Cli {
    /// Path to the ops.json spec.
    #[arg(short = 'p', long)]
    ops: String,

    /// Path to the Timely time log (time.tsv).
    #[arg(short = 't', long)]
    time: String,

    /// Path to the memory log (memory.tsv).
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

    // 1) Parse + validate ops.json (contains both topology + operator mapping).
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

    // Prepare node map keyed by stringified id for downstream rendering.
    let mut nodes_by_name = BTreeMap::new();
    for (id, node) in nodes {
        nodes_by_name.insert(id.to_string(), node);
    }

    let roots: Vec<String> = root_ids.iter().map(|id| id.to_string()).collect();

    let fingerprint_to_node: BTreeMap<String, String> = fingerprint_to_node
        .into_iter()
        .map(|(fp, id)| (fp, id.to_string()))
        .collect();

    // 2) Parse time log.
    let time_index = log::parse_time_file(&time)?;

    // 3) Parse memory log.
    let memory_index = log::parse_memory_file(&memory)?;

    // 4) Aggregate.
    let data = view::build_report_data(
        &nodes_by_name,
        &roots,
        &rules,
        &fingerprint_to_node,
        &time_index,
        &memory_index,
    )?;

    // 5) Render HTML.
    let html = render::render_html_report(&data)?;
    fs::write(&out, html)
        .with_context(|| diagnostics::error_message(format!("write output file {}", out)))?;
    println!("Wrote {}", out);

    Ok(())
}
