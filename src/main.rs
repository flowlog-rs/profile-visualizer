mod log;
mod model;
mod render;
mod spec;

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "flowlog-profiler")]
#[command(about = "FlowLog profiler report generator", long_about = None)]
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
        dag: String,

        #[arg(long)]
        ops: String,

        #[arg(short = 'o', long)]
        out: String,
    },
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    match cli.cmd {
        Commands::Report { log, dag, ops, out } => {
            // 1) Parse + validate specs.
            let dag_spec: spec::DagSpec = serde_json::from_str(&std::fs::read_to_string(&dag)?)?;
            let ops_spec: spec::OpsSpec = serde_json::from_str(&std::fs::read_to_string(&ops)?)?;

            let dag = dag_spec.validate_and_build()?;
            let name_ops = ops_spec.validate_and_build(&dag)?;

            // 2) Parse log.
            let log_index = log::parse_log_file(&log)?;

            // 3) Aggregate.
            let data = model::build_report_data(&dag, &name_ops, &log_index)?;

            // 4) Render HTML.
            let html = render::render_html_report(&data)?;
            std::fs::write(&out, html)?;
            eprintln!("Wrote {}", out);
        }
    }

    Ok(())
}
