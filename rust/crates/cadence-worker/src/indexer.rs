//! Visibility indexer lifecycle boundary; Kafka/ElasticSearch clients remain to port.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Mode {
    ElasticSearch,
    DualMigration,
}
