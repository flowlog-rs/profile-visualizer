//! Name -> operators mapping spec.
//!
//! Each "name" (a node in the NameDag) owns a set of operator addresses
//! from the Timely log. We will aggregate operator metrics per name.

use crate::spec::{Addr, NameDag};
use serde::Deserialize;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, Deserialize)]
pub struct OpsSpec {
    pub groups: Vec<NameGroupSpec>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct NameGroupSpec {
    pub name: String,
    pub operators: Vec<OperatorRefSpec>,
}

/// Operator references in ops.json.
///
/// MVP supports explicit addresses only.
/// We can extend later with prefix/wildcard/name filters.
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum OperatorRefSpec {
    Explicit { addr: Vec<u32> },
}

impl OpsSpec {
    /// Validate ops.json against dag:
    /// - group names must exist in dag
    /// - collects per-name operator addresses into a BTreeSet (dedup)
    pub fn validate_and_build(
        &self,
        dag: &NameDag,
    ) -> anyhow::Result<BTreeMap<String, BTreeSet<Addr>>> {
        use anyhow::bail;

        let mut map = BTreeMap::<String, BTreeSet<Addr>>::new();

        for g in &self.groups {
            if !dag.nodes.contains_key(&g.name) {
                bail!("ops.json references unknown name: {}", g.name);
            }

            let entry = map.entry(g.name.clone()).or_default();
            for op in &g.operators {
                match op {
                    OperatorRefSpec::Explicit { addr } => {
                        entry.insert(Addr::new(addr.clone()));
                    }
                }
            }
        }

        Ok(map)
    }
}
