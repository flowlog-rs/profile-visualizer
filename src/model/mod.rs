//! Aggregation model: combine name DAG + name->operators mapping + log rows.

use crate::log::{LogIndex, LogRow};
use crate::spec::{Addr, NameDag};
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, Serialize)]
pub struct OperatorView {
    pub addr: Vec<u32>,
    pub op_name: String,
    pub activations: u64,
    pub total_active_ms: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct NameNodeView {
    pub name: String,
    pub label: String,

    /// Primary tree children (spanning tree derived from DAG).
    pub children: Vec<String>,

    /// Additional parents beyond the chosen primary parent (for DAG info).
    pub extra_parents: Vec<String>,

    /// Aggregated over operators owned by this name.
    pub self_activations: u64,
    pub self_total_active_ms: f64,

    /// Operators owned by this name (sorted by total_active_ms desc).
    pub operators: Vec<OperatorView>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ReportData {
    pub roots: Vec<String>,
    pub nodes: BTreeMap<String, NameNodeView>,
    pub totals: TotalsView,
}

#[derive(Debug, Clone, Serialize)]
pub struct TotalsView {
    pub names: usize,
    pub operators_in_log: usize,
    pub operators_mapped: usize,
    pub total_mapped_ms: f64,
    pub total_mapped_activations: u64,
}

/// Build report data. Performs:
/// - detect operator addr assigned to multiple names (error)
/// - warn (stderr) about mapped addrs missing from log
pub fn build_report_data(
    dag: &NameDag,
    name_ops: &BTreeMap<String, BTreeSet<Addr>>,
    log: &LogIndex,
) -> anyhow::Result<ReportData> {
    use anyhow::bail;

    // 1) Enforce: each operator addr belongs to at most one name (strict).
    let mut owner: BTreeMap<&Addr, &str> = BTreeMap::new();
    for (name, ops) in name_ops {
        for addr in ops {
            if let Some(prev) = owner.insert(addr, name.as_str()) {
                bail!(
                    "operator addr {:?} is assigned to multiple names: {} and {}",
                    addr.0,
                    prev,
                    name
                );
            }
        }
    }

    // 2) Build a stable spanning tree from the DAG.
    // Choose a primary parent for each node: lexicographically smallest parent.
    let mut primary_parent: BTreeMap<String, Option<String>> = BTreeMap::new();
    let mut extra_parents: BTreeMap<String, Vec<String>> = BTreeMap::new();

    for name in dag.nodes.keys() {
        let mut ps = dag.parents.get(name).cloned().unwrap_or_default();
        ps.sort();
        if ps.is_empty() {
            primary_parent.insert(name.clone(), None);
            extra_parents.insert(name.clone(), vec![]);
        } else {
            let primary = ps[0].clone();
            let extras = ps[1..].to_vec();
            primary_parent.insert(name.clone(), Some(primary));
            extra_parents.insert(name.clone(), extras);
        }
    }

    let mut tree_children: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for (child, pp) in &primary_parent {
        if let Some(p) = pp {
            tree_children
                .entry(p.clone())
                .or_default()
                .push(child.clone());
        }
    }
    for kids in tree_children.values_mut() {
        kids.sort();
    }

    // Roots for the tree view: use dag.roots, plus any node that has no primary parent.
    // (dag.roots should usually cover it; this is a safe fallback.)
    let mut roots = dag.roots.clone();
    for (n, pp) in &primary_parent {
        if pp.is_none() && !roots.contains(n) {
            roots.push(n.clone());
        }
    }
    roots.sort();
    roots.dedup();

    // 3) Build per-name operator lists + aggregates.
    let mut nodes_view: BTreeMap<String, NameNodeView> = BTreeMap::new();

    let mut total_mapped_ms = 0.0f64;
    let mut total_mapped_activations = 0u64;
    let mut operators_mapped = 0usize;

    for (name, node) in &dag.nodes {
        let ops = name_ops.get(name).cloned().unwrap_or_default();

        let mut operators: Vec<OperatorView> = Vec::new();
        let mut self_ms = 0.0f64;
        let mut self_act = 0u64;

        for addr in &ops {
            match log.get(addr) {
                Some(LogRow {
                    addr,
                    activations,
                    total_active_ms,
                    op_name,
                }) => {
                    operators.push(OperatorView {
                        addr: addr.0.clone(),
                        op_name: op_name.clone(),
                        activations: *activations,
                        total_active_ms: *total_active_ms,
                    });
                    self_ms += *total_active_ms;
                    self_act += *activations;

                    total_mapped_ms += *total_active_ms;
                    total_mapped_activations += *activations;
                    operators_mapped += 1;
                }
                None => {
                    eprintln!(
                        "WARN: ops.json maps name '{}' to addr {:?}, but addr not found in log",
                        name, addr.0
                    );
                }
            }
        }

        // Sort operators by total_active_ms desc, then addr.
        operators.sort_by(|a, b| {
            b.total_active_ms
                .partial_cmp(&a.total_active_ms)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.addr.cmp(&b.addr))
        });

        nodes_view.insert(
            name.clone(),
            NameNodeView {
                name: name.clone(),
                label: node.label.clone(),
                children: tree_children.get(name).cloned().unwrap_or_default(),
                extra_parents: extra_parents.get(name).cloned().unwrap_or_default(),
                self_activations: self_act,
                self_total_active_ms: self_ms,
                operators,
            },
        );
    }

    Ok(ReportData {
        roots,
        totals: TotalsView {
            names: dag.nodes.len(),
            operators_in_log: log.len(),
            operators_mapped,
            total_mapped_ms,
            total_mapped_activations,
        },
        nodes: nodes_view,
    })
}
