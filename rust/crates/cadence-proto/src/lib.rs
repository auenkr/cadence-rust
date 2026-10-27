//! Cadence public and internal protobuf APIs, generated from upstream IDLs.
pub mod native;
pub mod task_list_partition;
pub mod task_list_status;
// Preserve generated tonic signatures, protobuf oneofs, and upstream IDL docs.
#[allow(
    clippy::result_large_err,
    clippy::large_enum_variant,
    clippy::doc_lazy_continuation
)]
pub mod uber {
    pub mod cadence {
        pub mod api {
            pub mod v1 {
                tonic::include_proto!("uber.cadence.api.v1");
                pub use crate::task_list_partition::api::TaskListPartitionConfig;
                pub use crate::task_list_status::TaskListStatus;
            }
        }
        pub mod admin {
            pub mod v1 {
                tonic::include_proto!("uber.cadence.admin.v1");
            }
        }
        pub mod history {
            pub mod v1 {
                tonic::include_proto!("uber.cadence.history.v1");
            }
        }
        pub mod matching {
            pub mod v1 {
                tonic::include_proto!("uber.cadence.matching.v1");
                pub use crate::task_list_partition::matching::TaskListPartitionConfig;
            }
        }
        pub mod shared {
            pub mod v1 {
                tonic::include_proto!("uber.cadence.shared.v1");
            }
        }
        pub mod indexer {
            pub mod v1 {
                tonic::include_proto!("uber.cadence.indexer.v1");
            }
        }
        pub mod sharddistributor {
            pub mod v1 {
                tonic::include_proto!("uber.cadence.sharddistributor.v1");
            }
        }
    }
}

pub use uber::cadence::{
    admin::v1 as admin, api::v1 as api, history::v1 as history, indexer::v1 as indexer,
    matching::v1 as matching, sharddistributor::v1 as sharddistributor, shared::v1 as shared,
};

pub const FILE_DESCRIPTOR_SET: &[u8] = tonic::include_file_descriptor_set!("cadence");

#[cfg(test)]
mod service_schema_tests {
    use prost::Message;
    use std::collections::{BTreeMap, BTreeSet};

    #[test]
    fn descriptor_contains_every_upstream_service_and_rpc() {
        let sources = [
            include_str!("../../../../go/idls/proto/uber/cadence/api/v1/service_domain.proto"),
            include_str!("../../../../go/idls/proto/uber/cadence/api/v1/service_meta.proto"),
            include_str!("../../../../go/idls/proto/uber/cadence/api/v1/service_workflow.proto"),
            include_str!("../../../../go/idls/proto/uber/cadence/api/v1/service_visibility.proto"),
            include_str!("../../../../go/idls/proto/uber/cadence/api/v1/service_worker.proto"),
            include_str!("../../../../go/idls/proto/uber/cadence/api/v1/service_schedule.proto"),
            include_str!("../../../../go/idls/proto/uber/cadence/api/v1/service_semaphore.proto"),
            include_str!("../../../../go/idls/proto/uber/cadence/admin/v1/service.proto"),
            include_str!("../../../../go/proto/internal/uber/cadence/history/v1/service.proto"),
            include_str!("../../../../go/proto/internal/uber/cadence/matching/v1/service.proto"),
            include_str!(
                "../../../proto/shard-manager/uber/cadence/sharddistributor/v1/service.proto"
            ),
            include_str!(
                "../../../proto/shard-manager/uber/cadence/sharddistributor/v1/executor.proto"
            ),
            include_str!(
                "../../../proto/shard-manager/uber/cadence/sharddistributor/v1/canary.proto"
            ),
        ];
        let mut expected = BTreeMap::new();
        for source in sources {
            let package = source
                .lines()
                .map(str::trim)
                .find_map(|line| line.strip_prefix("package "))
                .unwrap()
                .trim_end_matches(';');
            let service = source
                .lines()
                .map(str::trim)
                .find_map(|line| line.strip_prefix("service "))
                .unwrap()
                .split_whitespace()
                .next()
                .unwrap();
            let methods: BTreeSet<String> = source
                .lines()
                .map(str::trim)
                .filter_map(|line| line.strip_prefix("rpc "))
                .map(|line| line.split('(').next().unwrap().trim().to_owned())
                .collect();
            expected.insert(format!("{package}.{service}"), methods);
        }
        let descriptor =
            prost_types::FileDescriptorSet::decode(super::FILE_DESCRIPTOR_SET).unwrap();
        let mut actual = BTreeMap::new();
        for file in descriptor.file {
            for service in file.service {
                actual.insert(
                    format!(
                        "{}.{}",
                        file.package.as_deref().unwrap(),
                        service.name.as_deref().unwrap()
                    ),
                    service
                        .method
                        .into_iter()
                        .map(|method| method.name.unwrap())
                        .collect::<BTreeSet<_>>(),
                );
            }
        }
        assert_eq!(
            actual, expected,
            "missing or mismatched upstream service/RPC definitions"
        );
        assert_eq!(expected.len(), 13);
    }
}
