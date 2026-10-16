//! Encode nonnil partition map values explicitly, including empty partitions.
//! Pinned Go gogo/protobuf otherwise reads an omitted default value as nil.
macro_rules! partition_config {
    ($module:ident, $partition:path) => {
        #[allow(deprecated)]
        pub mod $module {
            use prost::{
                bytes::{Buf, BufMut},
                encoding::{DecodeContext, WireType},
                DecodeError, Message,
            };
            use std::collections::HashMap;
            type Partition = $partition;
            #[derive(Clone, Debug, Default, PartialEq)]
            pub struct TaskListPartitionConfig {
                pub version: i64,
                #[deprecated]
                pub num_read_partitions: i32,
                #[deprecated]
                pub num_write_partitions: i32,
                pub read_partitions: HashMap<i32, Partition>,
                pub write_partitions: HashMap<i32, Partition>,
            }
            #[derive(Clone, PartialEq, Message)]
            struct Entry {
                #[prost(int32, tag = "1")]
                key: i32,
                #[prost(message, optional, tag = "2")]
                value: Option<Partition>,
            }
            #[derive(Clone, PartialEq, Message)]
            struct WireConfig {
                #[prost(int64, tag = "1")]
                version: i64,
                #[prost(int32, tag = "2")]
                num_read_partitions: i32,
                #[prost(int32, tag = "3")]
                num_write_partitions: i32,
                #[prost(message, repeated, tag = "4")]
                read_partitions: Vec<Entry>,
                #[prost(message, repeated, tag = "5")]
                write_partitions: Vec<Entry>,
            }
            impl TaskListPartitionConfig {
                fn wire(&self) -> WireConfig {
                    let entries = |map: &HashMap<i32, Partition>| {
                        map.iter()
                            .map(|(key, value)| Entry {
                                key: *key,
                                value: Some(value.clone()),
                            })
                            .collect()
                    };
                    WireConfig {
                        version: self.version,
                        num_read_partitions: self.num_read_partitions,
                        num_write_partitions: self.num_write_partitions,
                        read_partitions: entries(&self.read_partitions),
                        write_partitions: entries(&self.write_partitions),
                    }
                }
                fn apply(&mut self, wire: WireConfig) {
                    self.version = wire.version;
                    self.num_read_partitions = wire.num_read_partitions;
                    self.num_write_partitions = wire.num_write_partitions;
                    self.read_partitions = wire
                        .read_partitions
                        .into_iter()
                        .map(|entry| (entry.key, entry.value.unwrap_or_default()))
                        .collect();
                    self.write_partitions = wire
                        .write_partitions
                        .into_iter()
                        .map(|entry| (entry.key, entry.value.unwrap_or_default()))
                        .collect();
                }
            }
            impl Message for TaskListPartitionConfig {
                fn encode_raw(&self, buf: &mut impl BufMut) {
                    self.wire().encode_raw(buf);
                }
                fn encoded_len(&self) -> usize {
                    self.wire().encoded_len()
                }
                fn clear(&mut self) {
                    *self = Self::default();
                }
                fn merge_field(
                    &mut self,
                    tag: u32,
                    wire_type: WireType,
                    buf: &mut impl Buf,
                    ctx: DecodeContext,
                ) -> Result<(), DecodeError> {
                    let mut wire = self.wire();
                    wire.merge_field(tag, wire_type, buf, ctx)?;
                    self.apply(wire);
                    Ok(())
                }
            }
        }
    };
}
partition_config!(api, crate::api::TaskListPartition);
partition_config!(matching, crate::matching::TaskListPartition);

#[cfg(test)]
mod tests {
    use prost::Message;
    #[test]
    fn partition_map_default_values_have_explicit_presence_on_both_apis() {
        let public = super::api::TaskListPartitionConfig {
            read_partitions: std::collections::HashMap::from([(
                0,
                crate::api::TaskListPartition::default(),
            )]),
            ..Default::default()
        };
        let internal = super::matching::TaskListPartitionConfig {
            read_partitions: std::collections::HashMap::from([(
                0,
                crate::matching::TaskListPartition::default(),
            )]),
            ..Default::default()
        };
        let bytes = vec![0x22, 2, 0x12, 0];
        assert_eq!(public.encode_to_vec(), bytes);
        assert_eq!(internal.encode_to_vec(), bytes);
        assert_eq!(
            super::api::TaskListPartitionConfig::decode(bytes.as_slice()).unwrap(),
            public
        );
        assert_eq!(
            super::matching::TaskListPartitionConfig::decode(bytes.as_slice()).unwrap(),
            internal
        );
        // Missing values decode to defaults; duplicate map keys use last value.
        let bytes = vec![0x22, 7, 0x12, 5, 0x0a, 3, b'o', b'l', b'd', 0x22, 0];
        assert_eq!(
            super::api::TaskListPartitionConfig::decode(bytes.as_slice()).unwrap(),
            public
        );
    }
}
