//! Generate Matching's transport-neutral, byte-valued string model from the
//! same descriptor set as the public protobuf contract. These are plain Rust
//! data types: they deliberately do not implement protobuf Message.

use heck::{ToSnakeCase, ToUpperCamelCase};
use prost::Message;
use prost_types::{DescriptorProto, FieldDescriptorProto, FileDescriptorSet};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write;
use std::path::Path;

#[path = "native_wire.rs"]
mod wire;

type Messages = BTreeMap<String, Definition>;

#[derive(Clone)]
struct Definition {
    descriptor: DescriptorProto,
    modules: Vec<String>,
    name: String,
}

fn collect(messages: &mut Messages, prefix: &str, modules: &[String], message: &DescriptorProto) {
    let name = message.name().to_upper_camel_case();
    let full = format!("{prefix}.{}", message.name());
    messages.insert(
        full.clone(),
        Definition {
            descriptor: message.clone(),
            modules: modules.to_vec(),
            name,
        },
    );
    let mut children = modules.to_vec();
    children.push(message.name().to_snake_case());
    for child in &message.nested_type {
        collect(messages, &full, &children, child);
    }
}

fn path(definition: &Definition, native: bool) -> String {
    format!(
        "crate::{}{}::{}",
        if native { "native::" } else { "" },
        definition.modules.join("::"),
        definition.name
    )
}

fn wrapper(name: &str) -> Option<&'static str> {
    match name {
        ".google.protobuf.StringValue" => Some("String"),
        ".google.protobuf.BytesValue" => Some("Vec<u8>"),
        ".google.protobuf.BoolValue" => Some("bool"),
        ".google.protobuf.Int32Value" => Some("i32"),
        ".google.protobuf.Int64Value" => Some("i64"),
        ".google.protobuf.UInt32Value" => Some("u32"),
        ".google.protobuf.UInt64Value" => Some("u64"),
        ".google.protobuf.FloatValue" => Some("f32"),
        ".google.protobuf.DoubleValue" => Some("f64"),
        ".google.protobuf.Empty" => Some("()"),
        _ => None,
    }
}

fn string_field(field: &FieldDescriptorProto) -> bool {
    field.r#type() as i32 == 9 || field.type_name() == ".google.protobuf.StringValue"
}

fn scalar(field: &FieldDescriptorProto, messages: &Messages) -> String {
    use prost_types::field_descriptor_proto::Type;
    if string_field(field) {
        return "::cadence_common::go_string::GoString".into();
    }
    match field.r#type() {
        Type::Double => "f64".into(),
        Type::Float => "f32".into(),
        Type::Int64 | Type::Sint64 | Type::Sfixed64 => "i64".into(),
        Type::Uint64 | Type::Fixed64 => "u64".into(),
        Type::Int32 | Type::Sint32 | Type::Sfixed32 | Type::Enum => "i32".into(),
        Type::Uint32 | Type::Fixed32 => "u32".into(),
        Type::Bool => "bool".into(),
        Type::Bytes => "Vec<u8>".into(),
        Type::Message => {
            if let Some(ty) = wrapper(field.type_name()) {
                return ty.into();
            }
            if let Some(name) = field.type_name().strip_prefix(".google.protobuf.") {
                return format!("::prost_types::{name}");
            }
            path(&messages[field.type_name()], true)
        }
        other => panic!("unsupported native field type {other:?}"),
    }
}

fn reaches(messages: &Messages, from: &str, target: &str, seen: &mut BTreeSet<String>) -> bool {
    if from == target {
        return true;
    }
    if !seen.insert(from.into()) {
        return false;
    }
    messages.get(from).is_some_and(|message| {
        message.descriptor.field.iter().any(|field| {
            field.r#type() as i32 == 11 && reaches(messages, field.type_name(), target, seen)
        })
    })
}

fn boxed(messages: &Messages, owner: &str, field: &FieldDescriptorProto) -> bool {
    field.r#type() as i32 == 11
        && !field.type_name().starts_with(".google.protobuf.")
        && reaches(messages, field.type_name(), owner, &mut BTreeSet::new())
}

fn map<'a>(
    messages: &'a Messages,
    field: &FieldDescriptorProto,
) -> Option<(&'a FieldDescriptorProto, &'a FieldDescriptorProto)> {
    let entry = messages.get(field.type_name())?;
    if !entry
        .descriptor
        .options
        .as_ref()
        .is_some_and(|options| options.map_entry())
    {
        return None;
    }
    Some((&entry.descriptor.field[0], &entry.descriptor.field[1]))
}

fn optional(field: &FieldDescriptorProto) -> bool {
    field.proto3_optional() || (field.r#type() as i32 == 11 && field.label() as i32 != 3)
}

fn field_type(messages: &Messages, owner: &str, field: &FieldDescriptorProto) -> String {
    if let Some((key, value)) = map(messages, field) {
        return format!(
            "::std::collections::HashMap<{}, {}>",
            scalar(key, messages),
            scalar(value, messages)
        );
    }
    let mut ty = scalar(field, messages);
    if field.label() as i32 == 3 {
        return format!("Vec<{ty}>");
    }
    if boxed(messages, owner, field) {
        ty = format!("Box<{ty}>");
    }
    if optional(field) {
        ty = format!("Option<{ty}>");
    }
    ty
}

fn atom(
    messages: &Messages,
    field: &FieldDescriptorProto,
    value: &str,
    outbound: bool,
    location: &str,
) -> String {
    if string_field(field) {
        return if outbound {
            format!("{value}.into_utf8().map_err(|error| crate::native::NativeStringError {{ field: {location:?}, bytes: error.into_bytes() }})?")
        } else {
            format!("{value}.into()")
        };
    }
    if field.r#type() as i32 == 11
        && messages.contains_key(field.type_name())
        && !field.type_name().starts_with(".google.protobuf.")
    {
        return format!(
            "{value}.{}",
            if outbound { "try_into()?" } else { "into()" }
        );
    }
    value.into()
}

fn conversion(
    messages: &Messages,
    owner: &str,
    field: &FieldDescriptorProto,
    value: &str,
    outbound: bool,
) -> String {
    let location = format!("{owner}.{}", field.name());
    if let Some((key, item)) = map(messages, field) {
        let key = atom(messages, key, "key", outbound, &format!("{location}.key"));
        let item = atom(
            messages,
            item,
            "item",
            outbound,
            &format!("{location}.value"),
        );
        return if outbound {
            format!("{value}.into_iter().map(|(key, item)| Ok(({key}, {item}))).collect::<Result<_, crate::native::NativeStringError>>()?")
        } else {
            format!("{value}.into_iter().map(|(key, item)| ({key}, {item})).collect()")
        };
    }
    if field.label() as i32 == 3 {
        let item = atom(messages, field, "item", outbound, &location);
        return if outbound {
            format!("{value}.into_iter().map(|item| Ok({item})).collect::<Result<_, crate::native::NativeStringError>>()?")
        } else {
            format!("{value}.into_iter().map(|item| {item}).collect()")
        };
    }
    let inner = if boxed(messages, owner, field) {
        format!(
            "Box::new({})",
            atom(messages, field, "(*item)", outbound, &location)
        )
    } else {
        atom(messages, field, "item", outbound, &location)
    };
    if optional(field) {
        return if outbound {
            format!("{value}.map(|item| Ok::<_, crate::native::NativeStringError>({inner})).transpose()?")
        } else {
            format!("{value}.map(|item| {inner})")
        };
    }
    atom(messages, field, value, outbound, &location)
}

#[derive(Default)]
struct Module {
    contents: String,
    children: BTreeMap<String, Module>,
}

impl Module {
    fn insert(&mut self, modules: &[String], contents: &str) {
        if let Some((first, remaining)) = modules.split_first() {
            self.children
                .entry(first.clone())
                .or_default()
                .insert(remaining, contents);
        } else {
            self.contents.push_str(contents);
        }
    }

    fn emit(&self, out: &mut String, prefix: &[String]) {
        if !prefix.is_empty() {
            writeln!(out, "pub use crate::{}::*;", prefix.join("::")).unwrap();
        }
        out.push_str(&self.contents);
        for (name, module) in &self.children {
            writeln!(out, "pub mod r#{name} {{").unwrap();
            let mut child = prefix.to_vec();
            child.push(name.clone());
            module.emit(out, &child);
            out.push_str("}\n");
        }
    }
}

pub fn generate(descriptor: &Path, output: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let set = FileDescriptorSet::decode(std::fs::read(descriptor)?.as_slice())?;
    let mut messages = Messages::new();
    for file in &set.file {
        let modules: Vec<_> = file.package().split('.').map(str::to_owned).collect();
        for message in &file.message_type {
            collect(
                &mut messages,
                &format!(".{}", file.package()),
                &modules,
                message,
            );
        }
    }
    let mut selected = BTreeSet::new();
    let mut pending: Vec<_> = messages
        .keys()
        .filter(|name| {
            name.starts_with(".uber.cadence.matching.v1.")
                || [
                    ".uber.cadence.history.v1.RecordDecisionTaskStartedRequest",
                    ".uber.cadence.history.v1.RecordDecisionTaskStartedResponse",
                    ".uber.cadence.history.v1.RecordActivityTaskStartedRequest",
                    ".uber.cadence.history.v1.RecordActivityTaskStartedResponse",
                    ".uber.cadence.history.v1.GetMutableStateRequest",
                    ".uber.cadence.history.v1.GetMutableStateResponse",
                    ".uber.cadence.history.v1.DescribeWorkflowExecutionRequest",
                    ".uber.cadence.history.v1.DescribeWorkflowExecutionResponse",
                    ".uber.cadence.history.v1.RefreshWorkflowTasksRequest",
                    ".uber.cadence.history.v1.RefreshWorkflowTasksResponse",
                ]
                .contains(&name.as_str())
        })
        .cloned()
        .collect();
    while let Some(name) = pending.pop() {
        if name.starts_with(".google.protobuf.") || !selected.insert(name.clone()) {
            continue;
        }
        for field in &messages[&name].descriptor.field {
            if field.r#type() as i32 == 11 {
                pending.push(field.type_name().to_owned());
            }
        }
    }
    let mut tree = Module::default();
    for name in &selected {
        let definition = &messages[name];
        let message = &definition.descriptor;
        if message
            .options
            .as_ref()
            .is_some_and(|options| options.map_entry())
        {
            continue;
        }
        // Oneof generation is explicit rather than treating alternative fields
        // as independent optionals, preserving the public API's last-wins union.
        let mut text = String::new();
        writeln!(
            text,
            "#[derive(Clone, Debug, Default, PartialEq)] pub struct {} {{",
            definition.name
        )?;
        for field in &message.field {
            if field.oneof_index.is_some() && !field.proto3_optional() {
                continue;
            }
            writeln!(
                text,
                "pub r#{}: {},",
                field.name().to_snake_case(),
                field_type(&messages, name, field)
            )?;
        }
        for (index, oneof) in message.oneof_decl.iter().enumerate() {
            if message
                .field
                .iter()
                .any(|field| field.oneof_index == Some(index as i32) && !field.proto3_optional())
            {
                let parent = format!("crate::native::{}", definition.modules.join("::"));
                writeln!(
                    text,
                    "pub r#{}: Option<{}::r#{}::{}>,",
                    oneof.name().to_snake_case(),
                    parent,
                    definition.name.to_snake_case(),
                    oneof.name().to_upper_camel_case()
                )?;
            }
        }
        text.push_str("}\n");
        for outbound in [false, true] {
            let source = path(definition, outbound);
            let target = path(definition, !outbound);
            let binding = if message.field.is_empty() {
                "_value"
            } else {
                "value"
            };
            if outbound {
                writeln!(text, "impl TryFrom<{source}> for {target} {{ type Error = crate::native::NativeStringError; fn try_from({binding}: {source}) -> Result<Self, Self::Error> {{ Ok(Self {{")?;
            } else {
                writeln!(text, "impl From<{source}> for {target} {{ fn from({binding}: {source}) -> Self {{ Self {{")?;
            }
            for field in &message.field {
                if field.oneof_index.is_some() && !field.proto3_optional() {
                    continue;
                }
                writeln!(
                    text,
                    "r#{}: {},",
                    field.name().to_snake_case(),
                    conversion(
                        &messages,
                        name,
                        field,
                        &format!("value.r#{}", field.name().to_snake_case()),
                        outbound
                    )
                )?;
            }
            for (index, oneof) in message.oneof_decl.iter().enumerate() {
                if message.field.iter().any(|field| {
                    field.oneof_index == Some(index as i32) && !field.proto3_optional()
                }) {
                    writeln!(
                        text,
                        "r#{}: value.r#{}.map(|item| {}).{},",
                        oneof.name().to_snake_case(),
                        oneof.name().to_snake_case(),
                        if outbound {
                            "item.try_into()"
                        } else {
                            "item.into()"
                        },
                        if outbound { "transpose()?" } else { "into()" }
                    )?;
                }
            }
            text.push_str(if outbound { "}) } }\n" } else { "} } }\n" });
        }
        text.push_str(&wire::implementation(&messages, definition));
        tree.insert(&definition.modules, &text);
        for (index, oneof) in message.oneof_decl.iter().enumerate() {
            let alternatives: Vec<_> = message
                .field
                .iter()
                .filter(|field| field.oneof_index == Some(index as i32) && !field.proto3_optional())
                .collect();
            if alternatives.is_empty() {
                continue;
            }
            let mut nested = definition.modules.clone();
            nested.push(definition.name.to_snake_case());
            let native_path = format!(
                "crate::native::{}::{}",
                nested.join("::"),
                oneof.name().to_upper_camel_case()
            );
            let proto_path = format!(
                "crate::{}::{}",
                nested.join("::"),
                oneof.name().to_upper_camel_case()
            );
            let mut code = format!(
                "#[derive(Clone, Debug, PartialEq)] pub enum {} {{\n",
                oneof.name().to_upper_camel_case()
            );
            for field in &alternatives {
                let mut ty = scalar(field, &messages);
                if boxed(&messages, name, field) {
                    ty = format!("Box<{ty}>");
                }
                writeln!(code, "{}({ty}),", field.name().to_upper_camel_case())?;
            }
            code.push_str("}\n");
            for outbound in [false, true] {
                let (source, target) = if outbound {
                    (&native_path, &proto_path)
                } else {
                    (&proto_path, &native_path)
                };
                if outbound {
                    writeln!(code, "impl TryFrom<{source}> for {target} {{ type Error = crate::native::NativeStringError; fn try_from(value: {source}) -> Result<Self, Self::Error> {{ Ok(match value {{")?;
                } else {
                    writeln!(code, "impl From<{source}> for {target} {{ fn from(value: {source}) -> Self {{ match value {{")?;
                }
                for field in &alternatives {
                    let variant = field.name().to_upper_camel_case();
                    let raw = if boxed(&messages, name, field) {
                        "(*item)"
                    } else {
                        "item"
                    };
                    let mut value = atom(
                        &messages,
                        field,
                        raw,
                        outbound,
                        &format!("{name}.{}", field.name()),
                    );
                    if boxed(&messages, name, field) {
                        value = format!("Box::new({value})");
                    }
                    writeln!(
                        code,
                        "{source}::{variant}(item) => {target}::{variant}({value}),"
                    )?;
                }
                code.push_str(if outbound { "}) } }\n" } else { "} } }\n" });
            }
            tree.insert(&nested, &code);
        }
    }
    let mut generated =
        String::from("// Generated from Matching's reachable protobuf descriptors.\n");
    tree.emit(&mut generated, &[]);
    for file in &set.file {
        if file.package() != "uber.cadence.matching.v1" {
            continue;
        }
        for service in &file.service {
            if service.name() != "MatchingAPI" {
                continue;
            }
            generated.push_str(
                "#[::tonic::async_trait]\npub trait NativeMatchingApi: Send + Sync + 'static {\n",
            );
            for method in &service.method {
                let input = path(&messages[method.input_type()], true);
                let output = path(&messages[method.output_type()], true);
                writeln!(generated, "async fn {}(&self, request: ::tonic::Request<{input}>) -> Result<::tonic::Response<{output}>, ::tonic::Status>;", method.name().to_snake_case())?;
            }
            generated.push_str("}\npub const NATIVE_MATCHING_METHODS: &[&str] = &[\n");
            for method in &service.method {
                writeln!(generated, "{:?},", method.name())?;
            }
            generated.push_str("];\n");
        }
    }
    generated.push_str("pub use uber::cadence::{api::v1 as api, matching::v1 as matching, history::v1 as history, shared::v1 as shared, admin::v1 as admin};\n");
    std::fs::write(output, generated)?;
    Ok(())
}
