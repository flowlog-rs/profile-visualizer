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
//!       "operators": [[0,1,2]],    // list of Timely operator addresses
//!       "parents": [1, 2]          // edges in the DAG (incoming)
//!     },
//!     ...
//!   ]
//! }
//!
//! We validate ids, turn operator address arrays into Addr, and compute roots
//! (nodes with no incoming edges).

use crate::Result;
use crate::addr::Addr;

use crate::diagnostics;

use anyhow::bail;
use serde::Deserialize;
use serde::de::Deserializer;
use std::collections::{BTreeMap, BTreeSet};

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
    pub operators: Vec<Addr>,

    #[serde(default)]
    pub parents: Vec<u32>,
}

/// Rule-level plan tree description keyed by fingerprints.
#[derive(Debug, Clone, Deserialize)]
pub struct RawRule {
    #[serde(default)]
    pub text: String,

    #[serde(default)]
    pub plan_tree: Vec<RawPlanNode>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RawPlanNode {
    #[serde(deserialize_with = "deserialize_fingerprint")]
    pub fingerprint: String,

    #[serde(default)]
    pub parents: Vec<String>,
}

/// Flattened, validated node ready for aggregation.
#[derive(Debug, Clone)]
pub struct NodeSpec {
    pub id: u32,
    pub label: String,
    pub block: String,
    pub fingerprint: Option<String>,
    pub tags: Vec<String>,
    pub parents: Vec<u32>,
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
    ///
    /// This function performs three major phases:
    /// 1) Normalize node rows (dedup parents, normalize fingerprints).
    /// 2) Validate structural integrity (unique ids, parents exist, fingerprint rules).
    /// 3) Build rule plan trees (derive children, compute roots).
    pub fn validate_and_build(&self) -> Result<ValidatedOps> {
        // Phase 1: build map keyed by id and normalize node fields.
        let mut nodes: BTreeMap<u32, NodeSpec> = BTreeMap::new();
        for raw in &self.nodes {
            if nodes.contains_key(&raw.id) {
                bail!(
                    "{}",
                    diagnostics::error_message(format!(
                        "duplicate node id in ops.json: {}",
                        raw.id
                    ))
                );
            }

            let block = raw.block.clone().unwrap_or_else(|| "other".to_string());

            let fingerprint = raw
                .fingerprint
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_string);

            let ops: BTreeSet<Addr> = raw.operators.iter().cloned().collect();
            let parents = normalize_parents(raw.parents.clone());

            nodes.insert(
                raw.id,
                NodeSpec {
                    id: raw.id,
                    label: raw.name.clone(),
                    block,
                    fingerprint,
                    tags: raw.tags.clone(),
                    parents,
                    operators: ops,
                },
            );
        }

        if nodes.is_empty() {
            bail!(
                "{}",
                diagnostics::error_message("ops.json contained no nodes")
            );
        }

        // Phase 2a: enforce unique, non-empty fingerprints within the same block.
        // We still record a global fingerprint->node mapping for rule validation.
        let mut fingerprint_to_node: BTreeMap<String, u32> = BTreeMap::new();
        let mut fingerprint_block_to_node: BTreeMap<(String, String), u32> = BTreeMap::new();
        for (id, node) in &nodes {
            if let Some(fp) = &node.fingerprint {
                let key = (node.block.clone(), fp.clone());
                if let Some(prev) = fingerprint_block_to_node.insert(key.clone(), *id) {
                    bail!(
                        "{}",
                        diagnostics::error_message(format!(
                            "fingerprint '{}' is used by multiple nodes in block '{}' ({} and {})",
                            fp, key.0, prev, id
                        ))
                    );
                }
                fingerprint_to_node.entry(fp.clone()).or_insert(*id);
            }
        }

        // Phase 2b: compute roots (nodes with no parents).
        let mut roots: Vec<u32> = Vec::new();
        for (id, node) in &nodes {
            if node.parents.is_empty() {
                roots.push(*id);
            }
        }
        roots.sort();
        roots.dedup();

        // Phase 2c: basic sanity—every parent id must exist.
        for node in nodes.values() {
            for pid in &node.parents {
                if !nodes.contains_key(pid) {
                    bail!(
                        "{}",
                        diagnostics::error_message(format!(
                            "node {} references missing parent id {}",
                            node.id, pid
                        ))
                    );
                }
            }
        }

        // Phase 3: validate rules + plan trees (if provided).
        let mut rules_out: Vec<RuleSpec> = Vec::new();
        for raw_rule in &self.rules {
            let mut raw_parents: BTreeMap<String, Vec<String>> = BTreeMap::new();
            let mut nodes_map: BTreeMap<String, RulePlanNodeSpec> = BTreeMap::new();

            for pn in &raw_rule.plan_tree {
                let fp = pn.fingerprint.trim();
                if fp.is_empty() {
                    bail!(
                        "{}",
                        diagnostics::error_message(format!(
                            "rule '{}' has an empty fingerprint entry",
                            raw_rule.text
                        ))
                    );
                }
                if nodes_map.contains_key(fp) {
                    bail!(
                        "{}",
                        diagnostics::error_message(format!(
                            "rule '{}' has duplicate fingerprint '{}' in plan tree",
                            raw_rule.text, fp
                        ))
                    );
                }
                if !fingerprint_to_node.contains_key(fp) {
                    bail!(
                        "{}",
                        diagnostics::error_message(format!(
                            "rule '{}' references fingerprint '{}' not found in any node",
                            raw_rule.text, fp
                        ))
                    );
                }

                let parents = normalize_parents(pn.parents.clone());
                raw_parents.insert(fp.to_string(), parents);
            }

            // Validate that all parents exist within the plan tree.
            for (fp, parents) in &raw_parents {
                for parent in parents {
                    if !raw_parents.contains_key(parent) {
                        bail!(
                            "{}",
                            diagnostics::error_message(format!(
                                "rule '{}' references parent fingerprint '{}' not present in its plan tree",
                                raw_rule.text, parent
                            ))
                        );
                    }
                }
                nodes_map.entry(fp.clone()).or_insert(RulePlanNodeSpec {
                    children: Vec::new(),
                });
            }

            // Derive children from parent edges (child <- parent).
            for (child, parents) in &raw_parents {
                for parent in parents {
                    nodes_map
                        .entry(parent.clone())
                        .or_insert(RulePlanNodeSpec {
                            children: Vec::new(),
                        })
                        .children
                        .push(child.clone());
                }
            }
            for node in nodes_map.values_mut() {
                node.children.sort();
                node.children.dedup();
            }

            // Compute sink (node with no children) and ensure exactly one.
            let sinks: Vec<String> = nodes_map
                .iter()
                .filter_map(|(fp, node)| node.children.is_empty().then(|| fp.clone()))
                .collect();

            if sinks.len() != 1 {
                bail!(
                    "{}",
                    diagnostics::error_message(format!(
                        "rule '{}' plan tree must have exactly one sink fingerprint (found {})",
                        raw_rule.text,
                        sinks.len()
                    ))
                );
            }

            let root_fp = sinks[0].clone();

            rules_out.push(RuleSpec {
                text: raw_rule.text.clone(),
                root: root_fp,
                nodes: nodes_map,
            });
        }

        // Phase 4: enforce that every fingerprinted node appears in rules.
        let mut rule_fps: BTreeSet<String> = BTreeSet::new();
        for rule in &rules_out {
            rule_fps.extend(rule.nodes.keys().cloned());
        }
        for (id, node) in &nodes {
            if let Some(fp) = &node.fingerprint {
                if !rule_fps.contains(fp) {
                    bail!(
                        "{}",
                        diagnostics::error_message(format!(
                            "node {} has fingerprint '{}' but it is not recorded in rules",
                            id, fp
                        ))
                    );
                }
            }
        }

        Ok(ValidatedOps {
            nodes,
            roots,
            rules: rules_out,
            fingerprint_to_node,
        })
    }
}

fn normalize_parents<T: Ord>(mut parents: Vec<T>) -> Vec<T> {
    // Sort + deduplicate to ensure stable ordering for output and comparisons.
    parents.sort();
    parents.dedup();
    parents
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
    // We disallow empty fingerprints because they are used as stable keys.
    let s = String::deserialize(deserializer)?;

    if s.trim().is_empty() {
        return Err(serde::de::Error::custom(diagnostics::error_message(
            "fingerprint cannot be empty",
        )));
    }
    Ok(s)
}
