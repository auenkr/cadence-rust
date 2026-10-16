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
    tonic_build::configure()
        .extern_path(".uber.cadence.api.v1.TaskListStatus", "crate::task_list_status::TaskListStatus")
        .extern_path(".uber.cadence.api.v1.TaskListPartitionConfig", "crate::task_list_partition::api::TaskListPartitionConfig")
        .extern_path(".uber.cadence.matching.v1.TaskListPartitionConfig", "crate::task_list_partition::matching::TaskListPartitionConfig")
        .generate_default_stubs(true)
        .file_descriptor_set_path(
            std::path::PathBuf::from(std::env::var("OUT_DIR")?).join("cadence.bin"),
        )
        .compile_protos(
            &files,
            &[
                root,
                internal,
                shard_manager,
                protoc_bin_vendored::include_path()?,
            ],
        )?;
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
