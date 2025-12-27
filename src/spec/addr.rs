//! Address type used in Timely logs.
//!
//! Example log addr: [0, 8, 10]  =>  Addr(vec![0, 8, 10])
//!
//! We store it as a Vec<u32> and derive ordering so it can be used in BTreeSet/Map.

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Addr(pub Vec<u32>);

impl Addr {
    pub fn new(path: Vec<u32>) -> Self {
        Self(path)
    }
}
