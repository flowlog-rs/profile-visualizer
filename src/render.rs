//! Report rendering (HTML).

use crate::Result;
use crate::view::ReportData;

use serde_json::to_string;

const TEMPLATE: &str = include_str!("../templates/report.html");

/// Render a self-contained HTML report (data embedded as JSON).
pub fn render_html_report(data: &ReportData) -> Result<String> {
    let json = to_string(data)?; // embedded as JS object literal
    Ok(TEMPLATE.replace("__DATA__", &json))
}
