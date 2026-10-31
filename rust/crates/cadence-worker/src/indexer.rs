//! Visibility indexer lifecycle boundary and document rules from Go `indexer`.

pub mod es_response;
pub mod fields;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Mode {
    ElasticSearch,
    DualMigration,
}
