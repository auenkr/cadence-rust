//! Generate a tonic service facade over the byte-valued native model.
use prost::Message;
use prost_types::FileDescriptorSet;
use std::{fmt::Write, path::Path};

pub fn generate(descriptor: &Path, output: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let set = FileDescriptorSet::decode(std::fs::read(descriptor)?.as_slice())?;
    let mut out = String::from("// Generated native Matching tonic bridge.\n");
    for file in &set.file {
        if file.package() != "uber.cadence.matching.v1" { continue; }
        for service in &file.service {
            if service.name() != "MatchingAPI" { continue; }
            writeln!(out, "#[::tonic::async_trait]\nimpl<T: crate::native::NativeMatchingApi> crate::native_service::matching_api_server::MatchingApi for T {{")?;
            for method in &service.method {
                let name = heck::ToSnakeCase::to_snake_case(method.name());
                let input = path(method.input_type());
                let output = path(method.output_type());
                writeln!(out, "async fn {name}(&self, request: ::tonic::Request<{input}>) -> Result<::tonic::Response<{output}>, ::tonic::Status> {{ crate::native::NativeMatchingApi::{name}(self, request).await }}")?;
            }
            out.push_str("}\n");
        }
    }
    std::fs::write(output, out)?;
    Ok(())
}

fn path(full: &str) -> String {
    let parts: Vec<_> = full.trim_start_matches('.').split('.').collect();
    format!("crate::native::{}", parts.join("::"))
}
