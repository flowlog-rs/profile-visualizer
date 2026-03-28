//! Report rendering (HTML).

use crate::Result;
use crate::view::ReportData;

use serde::Serialize;
use serde_json::to_string;

const TEMPLATE: &str = include_str!("../templates/report.html");

#[derive(Serialize)]
struct ReportWrapper<'a> {
    snapshot_labels: &'a [String],
    snapshots: &'a [ReportData],
}

/// Render a self-contained HTML report (data embedded as JSON).
pub fn render_html_report(labels: &[String], snapshots: &[ReportData]) -> Result<String> {
    let wrapper = ReportWrapper {
        snapshot_labels: labels,
        snapshots,
    };
    let json = to_string(&wrapper)?;
    Ok(TEMPLATE.replace("__DATA__", &json))
}
