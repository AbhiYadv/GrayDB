//! graydb-check: the invariant harness (built FIRST, per M4 philosophy).
//! Materialized(table, L) must be semantically equivalent to SourceSnapshot(table, L)
//! under the type-interpretation + eligibility contracts. Provides: snapshot differ,
//! randomized workload generator, fault scheduler (crash/kill/stall), per-SP demo drivers.

pub mod harness;
pub mod multiset;
