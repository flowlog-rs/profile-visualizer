//! Ops spec (ops.json) now provides a flat list of nodes plus edges.
//!
//! JSON shape:
//! {
//!   "nodes": [
//!     {
//!       "id": 0,
//!       "name": "foo",            // label rendered in UI
//!       "block": "input",        // grouping bucket for graph blocks
//!       "tags": ["Input"],        // optional, auxiliary
//!       "rule": "...",            // optional, unused today
//!       "operators": [[0,1,2]],    // list of Timely operator addresses
//!       "children": [1, 2]         // edges in the DAG
//!     },
//!     ...
//!   ]
//! }
//!
//! We validate ids, turn operator address arrays into Addr, derive parents, and
//! compute roots (nodes with no incoming edges).

use crate::spec::Addr;
use serde::Deserialize;
use std::collections::{BTreeMap, BTreeSet, HashMap};

#[derive(Debug, Clone, Deserialize)]
pub struct OpsSpec {
    #[serde(default)]
    pub nodes: Vec<RawNode>,
}

/// Raw node shape as it appears in ops.json.
#[derive(Debug, Clone, Deserialize)]
pub struct RawNode {
    pub id: u32,

    #[serde(default)]
    pub name: String,

    #[serde(default)]
    pub block: Option<String>,

    #[serde(default)]
    pub tags: Vec<String>,

    #[serde(default)]
    pub rule: Option<String>,

    #[serde(default)]
    pub operators: Vec<OperatorRefSpec>,

    #[serde(default)]
    pub children: Vec<u32>,
}

/// Flattened, validated node ready for aggregation.
#[derive(Debug, Clone)]
pub struct NodeSpec {
    pub id: u32,
    pub label: String,
    pub block: String,
    pub tags: Vec<String>,
    pub rule: Option<String>,
    pub children: Vec<u32>,
    pub operators: BTreeSet<Addr>,
}

/// Operator references in ops.json.
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum OperatorRefSpec {
    // New shape: operators: [[0,1,2]]
    Addr(Vec<u32>),
    // Backward compatibility: { "addr": [...] }
    Explicit { addr: Vec<u32> },
}

impl OpsSpec {
    /// Flatten all nodes, ensure unique ids, and compute roots.
    pub fn validate_and_build(&self) -> anyhow::Result<ValidatedOps> {
        use anyhow::bail;

        let raw_nodes = self.nodes.clone();

        // Build map keyed by id, check duplicates.
        let mut nodes: BTreeMap<u32, NodeSpec> = BTreeMap::new();
        for raw in raw_nodes {
            if nodes.contains_key(&raw.id) {
                bail!("duplicate node id in ops.json: {}", raw.id);
            }

            let block = raw
                .block
                .clone()
                .unwrap_or_else(|| "other".to_string());

            let mut ops = BTreeSet::new();
            for op in raw.operators {
                match op {
                    OperatorRefSpec::Addr(addr) | OperatorRefSpec::Explicit { addr } => {
                        ops.insert(Addr::new(addr));
                    }
                }
            }

            nodes.insert(
                raw.id,
                NodeSpec {
                    id: raw.id,
                    label: raw.name,
                    block,
                    tags: raw.tags,
                    rule: raw.rule,
                    children: raw.children,
                    operators: ops,
                },
            );
        }

        if nodes.is_empty() {
            bail!("ops.json contained no nodes");
        }

        // Compute parents map and roots from children edges.
        let mut parents: HashMap<u32, Vec<u32>> = HashMap::new();
        for (pid, node) in &nodes {
            for &cid in &node.children {
                parents.entry(cid).or_default().push(*pid);
            }
        }

        let mut roots: Vec<u32> = Vec::new();
        for id in nodes.keys() {
            if !parents.contains_key(id) {
                roots.push(*id);
            }
        }
        roots.sort();
        roots.dedup();

        // Basic sanity: every child id must exist.
        for node in nodes.values() {
            for cid in &node.children {
                if !nodes.contains_key(cid) {
                    bail!("node {} references missing child id {}", node.id, cid);
                }
            }
        }

        Ok(ValidatedOps { nodes, roots })
    }
}

#[derive(Debug, Clone)]
pub struct ValidatedOps {
    pub nodes: BTreeMap<u32, NodeSpec>,
    pub roots: Vec<u32>,
}
