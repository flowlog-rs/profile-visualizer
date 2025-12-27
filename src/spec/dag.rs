//! Name DAG spec and validation.
//!
//! The "name DAG" is your logical structure (prepare/core/post, etc.).
//! It can be a DAG; for MVP we only validate it is acyclic.
//!
//! We keep two representations:
//! - DagSpec: raw JSON input (serde-friendly)
//! - NameDag: validated and normalized in-memory structure

use serde::Deserialize;
use std::collections::BTreeMap;

#[derive(Debug, Clone, Deserialize)]
pub struct DagSpec {
    /// List of nodes. Node `name` must be unique.
    pub nodes: Vec<NameNodeSpec>,
    /// Directed edges: [src, dst]
    pub edges: Vec<[String; 2]>,
    /// Optional roots. If empty, roots are inferred by indegree==0.
    #[serde(default)]
    pub roots: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct NameNodeSpec {
    pub name: String,
    /// Optional display label. Defaults to `name`.
    #[serde(default)]
    pub label: Option<String>,
}

/// Validated + normalized name DAG representation.
#[derive(Debug, Clone)]
pub struct NameDag {
    pub nodes: BTreeMap<String, NameNode>, // name -> node
    pub children: BTreeMap<String, Vec<String>>,
    pub parents: BTreeMap<String, Vec<String>>,
    pub roots: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct NameNode {
    pub name: String,
    pub label: String,
}

impl DagSpec {
    /// Validate a DagSpec and build a NameDag:
    /// - unique node names
    /// - edges reference existing nodes
    /// - roots reference existing nodes (or inferred)
    /// - cycle detection (acyclic)
    pub fn validate_and_build(&self) -> anyhow::Result<NameDag> {
        use anyhow::{Context, bail};

        // 1) Unique nodes.
        let mut nodes = BTreeMap::<String, NameNode>::new();
        for n in &self.nodes {
            if nodes.contains_key(&n.name) {
                bail!("duplicate node name in dag.json: {}", n.name);
            }
            nodes.insert(
                n.name.clone(),
                NameNode {
                    name: n.name.clone(),
                    label: n.label.clone().unwrap_or_else(|| n.name.clone()),
                },
            );
        }
        if nodes.is_empty() {
            bail!("dag.json must contain at least 1 node");
        }

        // 2) Build adjacency maps.
        let mut children = BTreeMap::<String, Vec<String>>::new();
        let mut parents = BTreeMap::<String, Vec<String>>::new();
        for [src, dst] in &self.edges {
            if !nodes.contains_key(src) {
                bail!("edge references unknown src node: {}", src);
            }
            if !nodes.contains_key(dst) {
                bail!("edge references unknown dst node: {}", dst);
            }
            children.entry(src.clone()).or_default().push(dst.clone());
            parents.entry(dst.clone()).or_default().push(src.clone());
        }

        // 3) Roots.
        let roots = if !self.roots.is_empty() {
            for r in &self.roots {
                if !nodes.contains_key(r) {
                    bail!("roots references unknown node: {}", r);
                }
            }
            self.roots.clone()
        } else {
            nodes
                .keys()
                .filter(|n| parents.get(*n).map(|p| p.is_empty()).unwrap_or(true))
                .cloned()
                .collect::<Vec<_>>()
        };
        if roots.is_empty() {
            bail!("no roots found (graph may contain a cycle or all nodes have parents)");
        }

        // 4) Cycle detection (DFS coloring).
        #[derive(Copy, Clone, PartialEq, Eq)]
        enum Mark {
            Temp,
            Perm,
        }

        fn dfs(
            v: &str,
            children: &BTreeMap<String, Vec<String>>,
            marks: &mut BTreeMap<String, Mark>,
            stack: &mut Vec<String>,
        ) -> anyhow::Result<()> {
            use anyhow::bail;

            if let Some(Mark::Perm) = marks.get(v) {
                return Ok(());
            }
            if let Some(Mark::Temp) = marks.get(v) {
                // v is in the current recursion stack => cycle
                stack.push(v.to_string());
                bail!("cycle detected in dag.json: {}", stack.join(" -> "));
            }

            marks.insert(v.to_string(), Mark::Temp);
            stack.push(v.to_string());

            if let Some(kids) = children.get(v) {
                for k in kids {
                    dfs(k, children, marks, stack)?;
                }
            }

            stack.pop();
            marks.insert(v.to_string(), Mark::Perm);
            Ok(())
        }

        let mut marks = BTreeMap::<String, Mark>::new();
        let mut stack = Vec::<String>::new();
        for r in &roots {
            stack.clear();
            dfs(r, &children, &mut marks, &mut stack)
                .with_context(|| format!("cycle check failed starting at root {}", r))?;
        }

        Ok(NameDag {
            nodes,
            children,
            parents,
            roots,
        })
    }
}
