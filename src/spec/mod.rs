//! Spec layer: JSON schemas + validated in-memory structures.
//!
//! This module is intentionally separate from log parsing and rendering.
//! It owns:
//! - Addr type (operator id/path)
//! - Ops spec (UI tree + operator addresses)

pub mod addr;
pub mod ops;

pub use addr::Addr;
pub use ops::{NodeSpec, OpsSpec, RuleSpec, ValidatedOps};
