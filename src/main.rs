use clap::{Parser, Subcommand};

mod log;
mod model;
mod render;
mod spec;

pub type Result<T> = anyhow::Result<T>;

#[derive(Parser)]
#[command(name = "flowlog-profile-viz")]
#[command(about = "FlowLog profile visualizer", long_about = None)]
struct Cli {
    #[command(subcommand)]
    cmd: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Generate a profiling report (validates inputs while running).
    Report {
        #[arg(long)]
        log: String,

        #[arg(long)]
        ops: String,

        #[arg(short = 'o', long)]
        out: String,
    },
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.cmd {
        Commands::Report { log, ops, out } => {
            // 1) Parse + validate ops.json (contains both topology + operator mapping).
            let ops_spec: spec::OpsSpec = serde_json::from_str(&std::fs::read_to_string(&ops)?)?;
            let validated = ops_spec.validate_and_build()?;

            let spec::ValidatedOps {
                nodes,
                roots: root_ids,
                rules,
                fingerprint_to_node,
            } = validated;

            // Prepare node map keyed by stringified id for downstream rendering.
            let mut nodes_by_name = std::collections::BTreeMap::new();
            for (id, node) in nodes {
                nodes_by_name.insert(id.to_string(), node);
            }

            let roots: Vec<String> = root_ids.iter().map(|id| id.to_string()).collect();

            let fingerprint_to_node: std::collections::BTreeMap<String, String> = fingerprint_to_node
                .into_iter()
                .map(|(fp, id)| (fp, id.to_string()))
                .collect();

            // 2) Parse log.
            let log_index = log::parse_log_file(&log)?;

            // 3) Aggregate.
            let data = model::build_report_data(
                &nodes_by_name,
                &roots,
                &rules,
                &fingerprint_to_node,
                &log_index,
            )?;

            // 4) Render HTML.
            let html = render::render_html_report(&data)?;
            std::fs::write(&out, html)?;
            println!("Wrote {}", out);
        }
    }

    Ok(())
}
