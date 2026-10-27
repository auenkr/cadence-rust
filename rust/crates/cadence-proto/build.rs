#[path = "build/native_model.rs"]
mod native_model;
#[path = "build/native_service.rs"]
mod native_service;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../../go/idls/proto");
    let internal =
        std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../../go/proto/internal");
    let shard_manager =
        std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../proto/shard-manager");
    if !root.exists() {
        return Err("Cadence IDL missing: run git submodule update --init go/idls".into());
    }
    std::env::set_var("PROTOC", protoc_bin_vendored::protoc_bin_path()?);
    let mut files = Vec::new();
    collect_protos(&root, &mut files)?;
    collect_protos(&internal, &mut files)?;
    collect_protos(&shard_manager, &mut files)?;
    files.sort();
    let out = std::path::PathBuf::from(std::env::var("OUT_DIR")?);
    tonic_build::configure()
        .extern_path(
            ".uber.cadence.api.v1.TaskListStatus",
            "crate::task_list_status::TaskListStatus",
        )
        .extern_path(
            ".uber.cadence.api.v1.TaskListPartitionConfig",
            "crate::task_list_partition::api::TaskListPartitionConfig",
        )
        .extern_path(
            ".uber.cadence.matching.v1.TaskListPartitionConfig",
            "crate::task_list_partition::matching::TaskListPartitionConfig",
        )
        .generate_default_stubs(true)
        .file_descriptor_set_path(
            std::path::PathBuf::from(std::env::var("OUT_DIR")?).join("cadence.bin"),
        )
        .compile_protos(
            &files,
            &[
                root.clone(),
                internal.clone(),
                shard_manager.clone(),
                protoc_bin_vendored::include_path()?,
            ],
        )?;
    native_model::generate(&out.join("cadence.bin"), &out.join("matching_native.rs"))?;
    let native_out = out.join("native-service");
    std::fs::create_dir_all(&native_out)?;
    tonic_build::configure()
        .out_dir(&native_out)
        .extern_path(".uber.cadence.matching.v1", "crate::native::matching")
        .extern_path(".uber.cadence.api.v1", "crate::native::api")
        .extern_path(".uber.cadence.shared.v1", "crate::native::uber::cadence::shared::v1")
        .extern_path(".uber.cadence.history.v1", "crate::native::history")
        .extern_path(".google.protobuf", "::prost_types")
        .generate_default_stubs(true)
        .compile_protos(
            &[internal.join("uber/cadence/matching/v1/service.proto")],
            &[root.clone(), internal.clone(), shard_manager.clone(), protoc_bin_vendored::include_path()?],
        )?;
    native_service::generate(&out.join("cadence.bin"), &native_out.join("native_bridge.rs"))?;
    println!("cargo:rerun-if-changed=build/native_model.rs");
    println!("cargo:rerun-if-changed=build/native_wire.rs");
    println!("cargo:rerun-if-changed=build/native_service.rs");
    Ok(())
}

fn collect_protos(
    root: &std::path::Path,
    files: &mut Vec<std::path::PathBuf>,
) -> std::io::Result<()> {
    for entry in std::fs::read_dir(root)? {
        let path = entry?.path();
        if path.is_dir() {
            collect_protos(&path, files)?;
        } else if path.extension().is_some_and(|v| v == "proto") {
            files.push(path);
        }
    }
    Ok(())
}
