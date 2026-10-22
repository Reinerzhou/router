//! Stable conversation text used by cache-aware routing.
//!
//! This is a best-effort proxy for the prompt prefix. It deliberately ignores
//! request IDs, sampling options, metadata and session identifiers so repeated
//! conversations can stay on the worker holding their reusable prefix.

use serde_json::Value;

pub(crate) fn transparent_routing_text(path: &str, body: &Value) -> Option<String> {
    let mut parts = Vec::new();
    match path {
        "/v1/responses" => {
            if let Some(instructions) = body.get("instructions") {
                append_labeled(&mut parts, "instructions", instructions);
            }
            if let Some(tools) = body.get("tools") {
                append_tool_schemas(&mut parts, tools);
            }
            if let Some(input) = body.get("input") {
                append_responses_input(&mut parts, input);
            }
        }
        "/v1/messages" => {
            if let Some(system) = body.get("system") {
                append_labeled(&mut parts, "system", system);
            }
            if let Some(tools) = body.get("tools") {
                append_tool_schemas(&mut parts, tools);
            }
            if let Some(messages) = body.get("messages") {
                append_chat_messages(&mut parts, messages);
            }
        }
        _ => return None,
    }
    // A recognized endpoint with no extractable text must stay an empty key;
    // serializing the whole body would accidentally route on session IDs,
    // request IDs, sampling controls, or arbitrary metadata.
    Some(parts.join("\n"))
}

pub(crate) fn append_chat_messages(parts: &mut Vec<String>, messages: &Value) {
    let Some(messages) = messages.as_array() else {
        return;
    };
    for message in messages {
        append_message(parts, message);
    }
}

fn append_message(parts: &mut Vec<String>, message: &Value) {
    parts.push("message".to_string());
    // Keep role before text, so equal text from different roles doesn't look
    // like a full prompt-prefix hit. Escape strings and delimit messages.
    for field in [
        "role",
        "name",
        "reasoning",
        "content",
        "tool_calls",
        "function_call",
        "tool_call_id",
    ] {
        if let Some(value) = message.get(field) {
            append_labeled(parts, field, value);
        }
    }
    parts.push("end-message".to_string());
}

fn append_responses_input(parts: &mut Vec<String>, input: &Value) {
    match input {
        Value::String(_) => append_labeled(parts, "input", input),
        Value::Array(items) => {
            for item in items {
                if item.get("role").is_some() {
                    append_message(parts, item);
                } else {
                    // Retain function calls/results and opaque references as
                    // structured input; references don't reveal hidden history.
                    append_labeled(parts, "input_item", item);
                }
            }
        }
        _ => {}
    }
}

pub(crate) fn append_tool_schemas(parts: &mut Vec<String>, tools: &Value) {
    let Some(tools) = tools.as_array() else {
        return;
    };
    for tool in tools {
        // Template rendering can depend on tool order and type.
        append_labeled(parts, "tool_schema", tool);
    }
}

fn append_labeled(parts: &mut Vec<String>, label: &str, value: &Value) {
    match value {
        Value::String(text) => {
            if matches!(label, "image_url" | "url" | "file_data") && text.starts_with("data:") {
                parts.push(format!("field:{}", serde_json::to_string(label).unwrap()));
                append_hashed_image(parts, "data-url", text);
            } else {
                parts.push(format!(
                    "{}:{}",
                    serde_json::to_string(label).unwrap(),
                    serde_json::to_string(text).unwrap()
                ));
            }
        }
        Value::Array(values) => {
            parts.push(format!("array:{}", serde_json::to_string(label).unwrap()));
            for item in values {
                append_labeled(parts, "item", item);
            }
            parts.push("end-array".to_string());
        }
        Value::Object(fields) => {
            parts.push(format!("object:{}", serde_json::to_string(label).unwrap()));
            let mut keys = fields.keys().collect::<Vec<_>>();
            keys.sort();
            for key in keys {
                if key == "data" && value.get("type").and_then(Value::as_str) == Some("base64") {
                    if let Some(data) = value[key].as_str() {
                        let media = value
                            .get("media_type")
                            .and_then(Value::as_str)
                            .unwrap_or("");
                        parts.push("base64-data".to_string());
                        append_hashed_image(parts, media, data);
                        continue;
                    }
                }
                append_labeled(parts, key, &value[key]);
            }
            parts.push("end-object".to_string());
        }
        Value::Number(_) | Value::Bool(_) => {
            parts.push(format!("{}:{value}", serde_json::to_string(label).unwrap()))
        }
        Value::Null => parts.push(format!("{}:null", serde_json::to_string(label).unwrap())),
    }
}

fn append_hashed_image(parts: &mut Vec<String>, media_type: &str, data: &str) {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    media_type.hash(&mut hasher);
    data.hash(&mut hasher);
    parts.push(format!(
        "image:{}:{}:{:016x}",
        serde_json::to_string(media_type).unwrap(),
        data.len(),
        hasher.finish()
    ));
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocols::spec::{ChatCompletionRequest, GenerationRequest};

    fn chat_key(messages: Value) -> String {
        let request: ChatCompletionRequest =
            serde_json::from_value(serde_json::json!({"messages": messages})).unwrap();
        request.extract_text_for_routing()
    }

    #[test]
    fn tool_arguments_and_message_boundaries_affect_affinity() {
        let tool = |arguments| {
            chat_key(serde_json::json!([
                {"role":"assistant", "content":null, "tool_calls":[
                    {"id":"call-a", "type":"function", "function":{"name":"lookup", "arguments":arguments}}
                ]}
            ]))
        };
        assert_ne!(tool("Beijing"), tool("Shanghai"));
        assert_ne!(
            chat_key(serde_json::json!([{"role":"user", "content":"alpha\nassistant:beta"}])),
            chat_key(
                serde_json::json!([{"role":"user", "content":"alpha"}, {"role":"assistant", "content":"beta"}])
            )
        );
        assert_ne!(
            chat_key(serde_json::json!([{"role":"user", "content":" code "}])),
            chat_key(serde_json::json!([{"role":"user", "content":"code"}]))
        );
    }

    #[test]
    fn appended_history_keeps_previous_prefix_and_tool_order_is_preserved() {
        let first = chat_key(serde_json::json!([{"role":"user", "content":"hello"}]));
        let next = chat_key(serde_json::json!([
            {"role":"user", "content":"hello"},
            {"role":"assistant", "content":"hi"},
            {"role":"user", "content":"continue"}
        ]));
        assert!(next.starts_with(&first));
        let mut a = Vec::new();
        let mut b = Vec::new();
        append_tool_schemas(&mut a, &serde_json::json!([{"name":"a"},{"name":"b"}]));
        append_tool_schemas(&mut b, &serde_json::json!([{"name":"b"},{"name":"a"}]));
        assert_ne!(a, b);
    }

    #[test]
    fn responses_nested_images_and_tool_results_are_retained() {
        let image = |data| {
            transparent_routing_text(
                "/v1/responses",
                &serde_json::json!({
                    "input":[{"role":"user", "content":[
                        {"type":"input_text", "text":"describe"},
                        {"type":"input_image", "image_url":data}
                    ]}]
                }),
            )
            .unwrap()
        };
        let a = image("data:image/png;base64,AAAA");
        assert_ne!(a, image("data:image/png;base64,BBBB"));
        assert!(!a.contains("AAAA"));
        let result = |text| {
            transparent_routing_text(
                "/v1/responses",
                &serde_json::json!({
                    "input":[{"type":"function_call_output", "call_id":"call-a", "output":text}]
                }),
            )
        };
        assert_ne!(result("first"), result("second"));
    }

    #[test]
    fn chat_affinity_uses_messages_and_tools_not_session_or_sampling_fields() {
        let first = serde_json::json!({
            "session_params": {"session_id": "session-a"},
            "temperature": 0.1,
            "metadata": {"trace": "one"},
            "tools": [{"type": "function", "function": {"name": "lookup", "parameters": {"type": "object"}}}],
            "messages": [{"role": "user", "content": "find item"}]
        });
        let second = serde_json::json!({
            "session_params": {"session_id": "session-b"},
            "temperature": 0.9,
            "metadata": {"trace": "two"},
            "tools": [{"type": "function", "function": {"name": "lookup", "parameters": {"type": "object"}}}],
            "messages": [{"role": "user", "content": "find item"}]
        });

        let extract = |body: Value| {
            let request: ChatCompletionRequest = serde_json::from_value(body).unwrap();
            request.extract_text_for_routing()
        };
        let first_key = extract(first);
        assert_eq!(first_key, extract(second));
        assert!(first_key.contains("find item"));
        assert!(!first_key.contains("session-"));
    }

    #[test]
    fn responses_affinity_uses_visible_context_and_ignores_previous_response_id() {
        let body = serde_json::json!({
            "previous_response_id": "resp_123",
            "instructions": "Be concise",
            "input": "What is 2 + 2?",
            "temperature": 0.2,
            "metadata": {"trace": "ignored"}
        });
        let key = transparent_routing_text("/v1/responses", &body).unwrap();
        assert!(key.contains("Be concise"));
        assert!(key.contains("What is 2 + 2?"));
        assert!(!key.contains("resp_123"));
        assert!(!key.contains("ignored"));

        let id_only = serde_json::json!({"previous_response_id": "resp_123"});
        assert_eq!(
            transparent_routing_text("/v1/responses", &id_only),
            Some(String::new())
        );

        let file_image = serde_json::json!({
            "input": [{"type": "input_image", "file_id": "file_456"}]
        });
        assert!(transparent_routing_text("/v1/responses", &file_image)
            .unwrap()
            .contains("file_456"));
    }

    #[test]
    fn messages_affinity_uses_system_messages_and_tools() {
        let body = serde_json::json!({
            "system": "You are helpful",
            "tools": [{"name": "lookup", "input_schema": {"type": "object"}}],
            "messages": [{"role": "user", "content": [{"type": "text", "text": "find item"}]}],
            "max_tokens": 100,
            "metadata": {"trace": "ignored"}
        });
        let key = transparent_routing_text("/v1/messages", &body).unwrap();
        assert!(key.contains("You are helpful"));
        assert!(key.contains("find item"));
        assert!(key.contains("lookup"));
        assert!(!key.contains("ignored"));
        assert_eq!(
            transparent_routing_text("/v1/messages/count_tokens", &body),
            None
        );
    }

    #[test]
    fn messages_image_content_affects_affinity_without_embedding_base64() {
        let make_body = |data: &str| {
            serde_json::json!({
                "messages": [{"role": "user", "content": [
                    {"type": "text", "text": "describe this"},
                    {"type": "image", "source": {"type": "base64", "media_type": "image/png", "data": data}}
                ]}]
            })
        };
        let first = transparent_routing_text("/v1/messages", &make_body("image-one")).unwrap();
        let second = transparent_routing_text("/v1/messages", &make_body("image-two")).unwrap();
        assert_ne!(first, second);
        assert!(!first.contains("image-one"));
        assert!(first.contains("image/png"));
    }
}
