//! Emit prost wire implementations without passing string fields through UTF-8.
use super::*;

fn codec(field: &FieldDescriptorProto) -> String {
    use prost_types::field_descriptor_proto::Type;
    if field.type_name() == ".google.protobuf.StringValue" {
        return "crate::native::wire::string_value".into();
    }
    let name = match field.r#type() {
        Type::String => return "crate::native::wire::string".into(),
        Type::Enum | Type::Int32 => "int32",
        Type::Int64 => "int64",
        Type::Uint32 => "uint32",
        Type::Uint64 => "uint64",
        Type::Sint32 => "sint32",
        Type::Sint64 => "sint64",
        Type::Fixed32 => "fixed32",
        Type::Fixed64 => "fixed64",
        Type::Sfixed32 => "sfixed32",
        Type::Sfixed64 => "sfixed64",
        Type::Float => "float",
        Type::Double => "double",
        Type::Bool => "bool",
        Type::Bytes => "bytes",
        Type::Message => "message",
        Type::Group => "group",
    };
    format!("::prost::encoding::{name}")
}

fn packed(field: &FieldDescriptorProto) -> bool {
    use prost_types::field_descriptor_proto::Type;
    field.label() as i32 == 3
        && !matches!(
            field.r#type(),
            Type::String | Type::Bytes | Type::Message | Type::Group
        )
        && field
            .options
            .as_ref()
            .and_then(|options| options.packed)
            .unwrap_or(true)
}

fn oneof_path(definition: &Definition, field: &FieldDescriptorProto) -> String {
    let oneof = &definition.descriptor.oneof_decl[field.oneof_index.unwrap() as usize];
    format!(
        "crate::native::{}::{}::{}",
        definition.modules.join("::"),
        definition.name.to_snake_case(),
        oneof.name().to_upper_camel_case()
    )
}

fn present(field: &FieldDescriptorProto, value: &str) -> String {
    use prost_types::field_descriptor_proto::Type;
    match field.r#type() {
        Type::String | Type::Bytes => format!("!{value}.is_empty()"),
        Type::Bool => value.into(),
        Type::Float | Type::Double => format!("{value} != 0.0"),
        _ => format!("{value} != 0"),
    }
}

pub(super) fn implementation(messages: &Messages, definition: &Definition) -> String {
    let mut encode = String::new();
    let mut length = String::new();
    let mut merge = String::new();
    for field in &definition.descriptor.field {
        let tag = field.number();
        let name = format!("self.r#{}", field.name().to_snake_case());
        let wire = codec(field);
        if let Some(index) = field.oneof_index.filter(|_| !field.proto3_optional()) {
            let oneof = &definition.descriptor.oneof_decl[index as usize];
            let union = format!("self.r#{}", oneof.name().to_snake_case());
            let variant = format!(
                "{}::{}",
                oneof_path(definition, field),
                field.name().to_upper_camel_case()
            );
            writeln!(
                encode,
                "if let Some({variant}(value)) = &{union} {{ {wire}::encode({tag}, value, buf); }}"
            )
            .unwrap();
            writeln!(length, "if let Some({variant}(value)) = &{union} {{ len += {wire}::encoded_len({tag}, value); }}").unwrap();
            writeln!(merge, "{tag} => {{ if let Some({variant}(value)) = &mut {union} {{ {wire}::merge(wire_type, value, buf, ctx) }} else {{ let mut value = Default::default(); {wire}::merge(wire_type, &mut value, buf, ctx)?; {union} = Some({variant}(value)); Ok(()) }} }},").unwrap();
        } else if let Some((key, value)) = map(messages, field) {
            let message_value = value.r#type() as i32 == 11
                && !value.type_name().starts_with(".google.protobuf.");
            let key = codec(key);
            let value = codec(value);
            // Go always emits a nonnil message value, even when it is zero. prost
            // omits default map values, which a pinned Go decoder reads as nil.
            let module = if message_value { "crate::native::wire::message_map" } else { "::prost::encoding::hash_map" };
            writeln!(encode, "{module}::encode({key}::encode, {key}::encoded_len, {value}::encode, {value}::encoded_len, {tag}, &{name}, buf);").unwrap();
            writeln!(length, "len += {module}::encoded_len({key}::encoded_len, {value}::encoded_len, {tag}, &{name});").unwrap();
            writeln!(merge, "{tag} => {{ ::prost::encoding::check_wire_type(::prost::encoding::WireType::LengthDelimited, wire_type)?; ::prost::encoding::hash_map::merge({key}::merge, {value}::merge, &mut {name}, buf, ctx) }},").unwrap();
        } else if field.label() as i32 == 3 {
            if packed(field) {
                writeln!(encode, "{wire}::encode_packed({tag}, &{name}, buf);").unwrap();
                writeln!(length, "len += {wire}::encoded_len_packed({tag}, &{name});").unwrap();
                writeln!(
                    merge,
                    "{tag} => {wire}::merge_repeated(wire_type, &mut {name}, buf, ctx),"
                )
                .unwrap();
            } else {
                writeln!(
                    encode,
                    "for value in &{name} {{ {wire}::encode({tag}, value, buf); }}"
                )
                .unwrap();
                writeln!(
                    length,
                    "for value in &{name} {{ len += {wire}::encoded_len({tag}, value); }}"
                )
                .unwrap();
                // Numeric repeated fields accept both packed and unpacked
                // encodings, even when the descriptor chooses unpacked output.
                if matches!(field.r#type() as i32, 1..=8 | 13..=18) && field.r#type() as i32 != 11 {
                    writeln!(
                        merge,
                        "{tag} => {wire}::merge_repeated(wire_type, &mut {name}, buf, ctx),"
                    )
                    .unwrap();
                } else {
                    writeln!(merge, "{tag} => {{ let mut value = Default::default(); {wire}::merge(wire_type, &mut value, buf, ctx)?; {name}.push(value); Ok(()) }},").unwrap();
                }
            }
        } else if optional(field) {
            writeln!(
                encode,
                "if let Some(value) = &{name} {{ {wire}::encode({tag}, value, buf); }}"
            )
            .unwrap();
            writeln!(
                length,
                "if let Some(value) = &{name} {{ len += {wire}::encoded_len({tag}, value); }}"
            )
            .unwrap();
            writeln!(merge, "{tag} => {wire}::merge(wire_type, {name}.get_or_insert_with(Default::default), buf, ctx),").unwrap();
        } else {
            let condition = present(field, &name);
            writeln!(
                encode,
                "if {condition} {{ {wire}::encode({tag}, &{name}, buf); }}"
            )
            .unwrap();
            writeln!(
                length,
                "if {condition} {{ len += {wire}::encoded_len({tag}, &{name}); }}"
            )
            .unwrap();
            writeln!(
                merge,
                "{tag} => {wire}::merge(wire_type, &mut {name}, buf, ctx),"
            )
            .unwrap();
        }
    }
    let name = path(definition, true);
    format!("impl ::prost::Message for {name} {{
        fn encode_raw(&self, buf: &mut impl ::prost::bytes::BufMut) {{ {encode} }}
        fn encoded_len(&self) -> usize {{ let mut len = 0; {length} len }}
        fn merge_field(&mut self, tag: u32, wire_type: ::prost::encoding::WireType, buf: &mut impl ::prost::bytes::Buf, ctx: ::prost::encoding::DecodeContext) -> Result<(), ::prost::DecodeError> {{ match tag {{ {merge} _ => ::prost::encoding::skip_field(wire_type, tag, buf, ctx), }} }}
        fn clear(&mut self) {{ *self = Default::default(); }}
    }}\n")
}
