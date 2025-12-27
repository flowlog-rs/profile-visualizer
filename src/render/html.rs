use crate::model::ReportData;

/// Render a self-contained HTML report (data embedded as JSON).
///
/// Important: we avoid `format!()` because the HTML contains many `{}` from JS
/// template literals (e.g., `${x}`), which would conflict with Rust formatting.
pub fn render_html_report(data: &ReportData) -> anyhow::Result<String> {
    let json = serde_json::to_string(data)?; // embedded as JS object literal

    const TEMPLATE: &str = r#"<!doctype html>
<html>
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>FlowLog Profiler</title>
<style>
  body { font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif; margin: 0; }
  header { padding: 12px 16px; border-bottom: 1px solid #ddd; }
  .container { display: flex; height: calc(100vh - 58px); }
  .sidebar { width: 360px; border-right: 1px solid #ddd; padding: 12px; overflow: auto; }
  .main { flex: 1; padding: 12px; overflow: auto; }

  .summary { display: flex; gap: 16px; flex-wrap: wrap; font-size: 14px; color: #333; }
  .pill { padding: 4px 8px; border: 1px solid #ddd; border-radius: 999px; background: #fafafa; }

  .tree-node { cursor: pointer; user-select: none; padding: 2px 4px; border-radius: 4px; }
  .tree-node:hover { background: #f3f3f3; }
  .tree-node.selected { background: #e9f2ff; border: 1px solid #cfe3ff; }
  .indent { display: inline-block; width: 16px; }
  .toggle { display: inline-block; width: 16px; text-align: center; color: #666; }
  .muted { color: #777; font-size: 12px; }

  table { border-collapse: collapse; width: 100%; margin-top: 8px; }
  th, td { border-bottom: 1px solid #eee; padding: 6px 8px; text-align: left; font-size: 14px; }
  th { position: sticky; top: 0; background: white; border-bottom: 1px solid #ddd; }
  .num { text-align: right; font-variant-numeric: tabular-nums; }
  code { font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; font-size: 13px; }
</style>
</head>
<body>
<header>
  <div class="summary" id="summary"></div>
</header>

<div class="container">
  <div class="sidebar">
    <div style="display:flex; gap: 8px; margin-bottom: 8px;">
      <input id="search" placeholder="Search name..." style="flex:1; padding: 6px 8px; border: 1px solid #ddd; border-radius: 6px;">
      <button id="expandAll" style="padding: 6px 10px;">Expand</button>
      <button id="collapseAll" style="padding: 6px 10px;">Collapse</button>
    </div>
    <div id="tree"></div>
  </div>

  <div class="main">
    <h2 id="title">Select a node</h2>
    <div id="meta" class="muted"></div>

    <table id="opsTable" style="display:none;">
      <thead>
        <tr>
          <th>addr</th>
          <th>operator</th>
          <th class="num">activations</th>
          <th class="num">total_active_ms</th>
        </tr>
      </thead>
      <tbody id="opsBody"></tbody>
    </table>
  </div>
</div>

<script>
// Embedded report data (JSON object literal)
const DATA = __DATA__;

const state = {
  expanded: new Set(),
  selected: null,
  search: ""
};

function fmtMs(x) {
  return (Math.round(x * 1000) / 1000).toFixed(3);
}

function escapeHtml(s) {
  return String(s)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function renderSummary() {
  const t = DATA.totals;
  const el = document.getElementById("summary");
  el.innerHTML = `
    <span class="pill">names: <b>${t.names}</b></span>
    <span class="pill">operators in log: <b>${t.operators_in_log}</b></span>
    <span class="pill">operators mapped: <b>${t.operators_mapped}</b></span>
    <span class="pill">mapped ms: <b>${fmtMs(t.total_mapped_ms)}</b></span>
    <span class="pill">mapped activations: <b>${t.total_mapped_activations}</b></span>
  `;
}

function nodeMatches(name, node) {
  if (!state.search) return true;
  const s = state.search.toLowerCase();
  return name.toLowerCase().includes(s) || (node.label || "").toLowerCase().includes(s);
}

function renderTree() {
  const root = document.getElementById("tree");
  root.innerHTML = "";

  // If search is active, show matches + ancestors in the spanning tree.
  const mustShow = new Set();
  if (state.search) {
    const parent = new Map();
    for (const [name, node] of Object.entries(DATA.nodes)) {
      for (const c of node.children) parent.set(c, name);
    }
    for (const [name, node] of Object.entries(DATA.nodes)) {
      if (nodeMatches(name, node)) {
        let cur = name;
        while (cur) {
          mustShow.add(cur);
          cur = parent.get(cur);
        }
      }
    }
  }

  function renderSubtree(name, depth) {
    const node = DATA.nodes[name];
    if (!node) return;

    if (state.search && !mustShow.has(name)) return;

    const isExpanded = state.expanded.has(name);
    const hasKids = node.children && node.children.length > 0;

    const row = document.createElement("div");
    row.className = "tree-node" + (state.selected === name ? " selected" : "");
    row.onclick = () => selectNode(name);

    const indent = document.createElement("span");
    indent.className = "indent";
    indent.style.width = (depth * 16) + "px";
    row.appendChild(indent);

    const toggle = document.createElement("span");
    toggle.className = "toggle";
    toggle.textContent = hasKids ? (isExpanded ? "▾" : "▸") : " ";
    toggle.onclick = (e) => {
      e.stopPropagation();
      if (!hasKids) return;
      if (isExpanded) state.expanded.delete(name);
      else state.expanded.add(name);
      renderTree();
    };
    row.appendChild(toggle);

    const label = document.createElement("span");
    label.innerHTML = `${escapeHtml(node.label)} <span class="muted">(${fmtMs(node.self_total_active_ms)} ms, ${node.self_activations} act)</span>`;
    row.appendChild(label);

    root.appendChild(row);

    if (hasKids && isExpanded) {
      for (const c of node.children) renderSubtree(c, depth + 1);
    }
  }

  for (const r of DATA.roots) renderSubtree(r, 0);
}

function selectNode(name) {
  state.selected = name;
  const node = DATA.nodes[name];
  document.getElementById("title").textContent = node.label;

  const extra = (node.extra_parents && node.extra_parents.length)
    ? (" extra parents: " + node.extra_parents.join(", "))
    : "";

  document.getElementById("meta").textContent =
    `name: ${name} | self: ${fmtMs(node.self_total_active_ms)} ms | activations: ${node.self_activations}` + extra;

  const tbl = document.getElementById("opsTable");
  const body = document.getElementById("opsBody");
  body.innerHTML = "";

  if (!node.operators || node.operators.length === 0) {
    tbl.style.display = "none";
  } else {
    tbl.style.display = "table";
    for (const op of node.operators) {
      const tr = document.createElement("tr");
      tr.innerHTML = `
        <td><code>[${op.addr.join(", ")}]</code></td>
        <td>${escapeHtml(op.op_name)}</td>
        <td class="num">${op.activations}</td>
        <td class="num">${fmtMs(op.total_active_ms)}</td>
      `;
      body.appendChild(tr);
    }
  }

  renderTree();
}

function expandAll() {
  for (const name of Object.keys(DATA.nodes)) {
    const node = DATA.nodes[name];
    if (node.children && node.children.length) state.expanded.add(name);
  }
  renderTree();
}

function collapseAll() {
  state.expanded.clear();
  renderTree();
}

document.getElementById("search").addEventListener("input", (e) => {
  state.search = e.target.value || "";
  renderTree();
});

document.getElementById("expandAll").onclick = expandAll;
document.getElementById("collapseAll").onclick = collapseAll;

renderSummary();
for (const r of DATA.roots) state.expanded.add(r);
renderTree();
if (DATA.roots.length) selectNode(DATA.roots[0]);
</script>
</body>
</html>
"#;

    Ok(TEMPLATE.replace("__DATA__", &json))
}
