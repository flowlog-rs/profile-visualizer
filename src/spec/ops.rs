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
use crate::Result;
use anyhow::bail;
use serde::Deserialize;
use serde::de::Deserializer;
use std::collections::{BTreeMap, BTreeSet, HashMap};

#[derive(Debug, Clone, Deserialize)]
pub struct OpsSpec {
    #[serde(default)]
    pub nodes: Vec<RawNode>,

    #[serde(default)]
    pub rules: Vec<RawRule>,
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
    pub fingerprint: Option<String>,

    #[serde(default)]
    pub tags: Vec<String>,

    #[serde(default)]
    pub rule: Option<String>,

    #[serde(default)]
    pub operators: Vec<Addr>,

    #[serde(default)]
    pub children: Vec<u32>,
}

/// Rule-level plan tree description keyed by fingerprints.
#[derive(Debug, Clone, Deserialize)]
pub struct RawRule {
    #[serde(default)]
    pub text: String,

    #[serde(default)]
    pub plantree: Vec<RawPlanNode>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RawPlanNode {
    #[serde(deserialize_with = "deserialize_fingerprint")]
    pub fingerprints: String,

    #[serde(default)]
    pub children: Vec<String>,
}

/// Flattened, validated node ready for aggregation.
#[derive(Debug, Clone)]
pub struct NodeSpec {
    pub id: u32,
    pub label: String,
    pub block: String,
    pub fingerprint: Option<String>,
    pub tags: Vec<String>,
    pub rule: Option<String>,
    pub children: Vec<u32>,
    pub operators: BTreeSet<Addr>,
}

#[derive(Debug, Clone)]
pub struct RulePlanNodeSpec {
    pub children: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct RuleSpec {
    pub text: String,
    pub root: String,
    pub nodes: BTreeMap<String, RulePlanNodeSpec>,
}

impl OpsSpec {
    /// Flatten all nodes, ensure unique ids, and compute roots.
    pub fn validate_and_build(&self) -> Result<ValidatedOps> {
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

            let fingerprint = raw
                .fingerprint
                .as_ref()
                .map(|f| f.trim().to_string())
                .filter(|s| !s.is_empty());

            let mut ops = BTreeSet::new();
            for addr in raw.operators {
                ops.insert(addr);
            }

            nodes.insert(
                raw.id,
                NodeSpec {
                    id: raw.id,
                    label: raw.name,
                    block,
                    fingerprint,
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

        // Enforce unique, non-empty fingerprints where present.
        let mut fingerprint_to_node: BTreeMap<String, u32> = BTreeMap::new();
        for (id, node) in &nodes {
            if let Some(fp) = &node.fingerprint {
                let fp_trim = fp.trim();
                if fp_trim.is_empty() {
                    bail!("node {} has an empty fingerprint", id);
                }
                if let Some(prev) = fingerprint_to_node.insert(fp_trim.to_string(), *id) {
                    bail!(
                        "fingerprint '{}' is used by multiple nodes ({} and {})",
                        fp_trim,
                        prev,
                        id
                    );
                }
            }
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

        // Validate rules + plan trees (if provided).
        let mut rules_out: Vec<RuleSpec> = Vec::new();
        for raw_rule in &self.rules {
            let mut nodes_map: BTreeMap<String, RulePlanNodeSpec> = BTreeMap::new();

            for pn in &raw_rule.plantree {
                let fp = pn.fingerprints.trim();
                if fp.is_empty() {
                    bail!("rule '{}' has an empty fingerprint entry", raw_rule.text);
                }
                if nodes_map.contains_key(fp) {
                    bail!(
                        "rule '{}' has duplicate fingerprint '{}' in plan tree",
                        raw_rule.text,
                        fp
                    );
                }
                if !fingerprint_to_node.contains_key(fp) {
                    bail!(
                        "rule '{}' references fingerprint '{}' not found in any node",
                        raw_rule.text,
                        fp
                    );
                }

                nodes_map.insert(
                    fp.to_string(),
                    RulePlanNodeSpec {
                        children: pn.children.clone(),
                    },
                );
            }

            // Validate that all children exist within the plan tree.
            for node in nodes_map.values() {
                for child in &node.children {
                    if !nodes_map.contains_key(child) {
                        bail!(
                            "rule '{}' references child fingerprint '{}' not present in its plan tree",
                            raw_rule.text,
                            child
                        );
                    }
                }
            }

            // Compute sinks (nodes with no children) and ensure exactly one.
            let sinks: Vec<String> = nodes_map
                .iter()
                .filter_map(|(fp, node)| if node.children.is_empty() { Some(fp.clone()) } else { None })
                .collect();

            if sinks.len() != 1 {
                bail!(
                    "rule '{}' plan tree must have exactly one sink/leaf fingerprint (found {})",
                    raw_rule.text,
                    sinks.len()
                );
            }

            let root_fp = sinks[0].clone();

            // Compute parents for shared-node detection.
            let mut parents: HashMap<String, Vec<String>> = HashMap::new();
            for (fp, node) in &nodes_map {
                for child in &node.children {
                    parents.entry(child.clone()).or_default().push(fp.clone());
                }
            }

            rules_out.push(RuleSpec {
                text: raw_rule.text.clone(),
                root: root_fp,
                nodes: nodes_map,
            });
        }

        Ok(ValidatedOps {
            nodes,
            roots,
            rules: rules_out,
            fingerprint_to_node,
        })
    }
}

#[derive(Debug, Clone)]
pub struct ValidatedOps {
    pub nodes: BTreeMap<u32, NodeSpec>,
    pub roots: Vec<u32>,
    pub rules: Vec<RuleSpec>,
    pub fingerprint_to_node: BTreeMap<String, u32>,
}

fn deserialize_fingerprint<'de, D>(deserializer: D) -> std::result::Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;

    if s.trim().is_empty() {
        return Err(serde::de::Error::custom("fingerprint cannot be empty"));
    }
    Ok(s)
}
