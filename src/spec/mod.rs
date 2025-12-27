//! Spec layer: JSON schemas + validated in-memory structures.
//!
//! This module is intentionally separate from log parsing and rendering.
//! It owns:
//! - Addr type (operator id/path)
//! - DAG spec (name graph)
//! - Ops spec (name -> set of operator addrs)

pub mod addr;
pub mod dag;
pub mod ops;

pub use addr::Addr;
pub use dag::{DagSpec, NameDag, NameNode, NameNodeSpec};
pub use ops::{NameGroupSpec, OperatorRefSpec, OpsSpec};
