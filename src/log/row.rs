use crate::spec::Addr;
use std::collections::BTreeMap;

/// A single operator row from the Timely profile table.
#[derive(Debug, Clone)]
pub struct LogRow {
    pub addr: Addr,
    pub activations: u64,
    pub total_active_ms: f64,
    pub op_name: String,
}

/// Index by address for fast lookup during aggregation.
pub type LogIndex = BTreeMap<Addr, LogRow>;
