//! Stable conversation text used by cache-aware routing.
//!
//! This is a best-effort proxy for the prompt prefix. It deliberately ignores
//! request IDs, sampling options, metadata and session identifiers so repeated
//! conversations can stay on the worker holding their reusable prefix.

use serde_json::Value;

pub(crate) fn transparent_routing_text(path: &str, body: &Value) -> Option<String> {
    match path {
        "/v1/responses" => {
            // Backend-held history is invisible to this router. Never infer a
            // cache prefix from only the visible tail of such a request.
            if body
                .get("previous_response_id")
                .is_some_and(|v| !v.is_null())
                || body.get("conversation").is_some_and(|v| !v.is_null())
                || body.get("input").is_some_and(has_item_reference)
            {
                return Some(String::new());
            }
            let mut visible = body.clone();
            if let Some(items) = visible.get_mut("input").and_then(Value::as_array_mut) {
                for item in items {
                    if item.get("type").and_then(Value::as_str) == Some("message") {
                        if let Some(fields) = item.as_object_mut() {
                            fields.remove("id"); // Response item ID does not render into the prompt.
                        }
                    }
                }
            }
            Some(conservative_key(&visible, "input"))
        }
        "/v1/messages" => Some(conservative_key(body, "messages")),
        _ => None,
    }
}

pub(crate) fn conservative_key(body: &Value, prompt_field: &str) -> String {
    let Some(items) = body.get(prompt_field) else {
        return String::new();
    };
    let messages = match items {
        Value::Array(items) => items.iter().collect::<Vec<_>>(),
        Value::Null => return String::new(),
        item => vec![item],
    };
    let Some(first) = messages.first() else {
        return String::new();
    };

    // A full rendering configuration and a full first message must agree
    // before any prefix is counted. This prevents long tools or arguments
    // serialized in a different order from masquerading as a prompt prefix.
    let mut config = body.clone();
    if let Some(fields) = config.as_object_mut() {
        fields.remove(prompt_field);
        for field in [
            "temperature",
            "top_p",
            "top_k",
            "min_p",
            "max_tokens",
            "max_completion_tokens",
            "max_output_tokens",
            "min_tokens",
            "n",
            "stream",
            "stream_options",
            "seed",
            "stop",
            "stop_sequences",
            "stop_token_ids",
            "presence_penalty",
            "frequency_penalty",
            "repetition_penalty",
            "logit_bias",
            "logprobs",
            "top_logprobs",
            "metadata",
            "user",
            "session_id",
            "session_params",
            "request_id",
            "store",
            "service_tier",
            "prompt_cache_key",
            "client_metadata",
        ] {
            fields.remove(field);
        }
    }
    use std::hash::{Hash, Hasher};
    let hash = |value: &Value| {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        value.to_string().hash(&mut hasher);
        format!("{:016x}", hasher.finish())
    };
    let mut first_group = serde_json::Map::new();
    first_group.insert("config".to_string(), config.clone());
    first_group.insert("first".to_string(), (*first).clone());
    let mut parts = vec![hash(&Value::Object(first_group))];
    append_labeled(&mut parts, "config", &config);
    append_labeled(&mut parts, "message", first);
    for message in messages.iter().skip(1) {
        parts.push(hash(message));
        append_labeled(&mut parts, "message", message);
    }
    parts.join("\n")
}

fn has_item_reference(value: &Value) -> bool {
    match value {
        Value::Array(items) => items.iter().any(has_item_reference),
        Value::Object(fields) => {
            fields.get("type").and_then(Value::as_str) == Some("item_reference")
                || fields.values().any(has_item_reference)
        }
        _ => false,
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
        let a = conservative_key(
            &serde_json::json!({"tools":[{"name":"a"},{"name":"b"}], "messages":[{"role":"user","content":"hello"}]}),
            "messages",
        );
        let b = conservative_key(
            &serde_json::json!({"tools":[{"name":"b"},{"name":"a"}], "messages":[{"role":"user","content":"hello"}]}),
            "messages",
        );
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
    fn responses_affinity_requires_explicit_history() {
        let body = serde_json::json!({
            "instructions": "Be concise",
            "input": "What is 2 + 2?",
            "temperature": 0.2,
            "metadata": {"trace": "ignored"}
        });
        let key = transparent_routing_text("/v1/responses", &body).unwrap();
        assert!(key.contains("Be concise"));
        assert!(key.contains("What is 2 + 2?"));
        assert!(!key.contains("ignored"));

        let id_only = serde_json::json!({"previous_response_id": "resp_123", "input": "continue"});
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

    #[test]
    fn changed_system_or_tool_call_name_cannot_match_long_later_fields() {
        let tools = serde_json::json!([{"name":"lookup", "description":"x".repeat(10_000)}]);
        let make = |system: &str| {
            conservative_key(
                &serde_json::json!({
                    "tools":tools, "messages":[{"role":"system", "content":system}]
                }),
                "messages",
            )
        };
        let first = make("A");
        let second = make("B");
        assert_ne!(first.lines().next(), second.lines().next());

        let call = |name: &str| {
            conservative_key(
                &serde_json::json!({
                    "messages":[{"role":"assistant", "tool_calls":[{
                        "type":"function", "function":{"name":name, "arguments":"x".repeat(10_000)}
                    }]}]
                }),
                "messages",
            )
        };
        assert_ne!(call("first").lines().next(), call("second").lines().next());
    }

    #[test]
    fn prompt_configuration_isolated_and_hidden_responses_history_is_a_miss() {
        let make = |choice: &str| {
            conservative_key(
                &serde_json::json!({
                    "tools":[{"name":"lookup"}], "tool_choice":choice,
                    "messages":[{"role":"user", "content":"same"}]
                }),
                "messages",
            )
        };
        assert_ne!(make("none").lines().next(), make("auto").lines().next());
        for body in [
            serde_json::json!({"previous_response_id":"resp_A", "input":"continue"}),
            serde_json::json!({"conversation":"conv_A", "input":"continue"}),
            serde_json::json!({"input":[{"type":"item_reference", "id":"item_A"}]}),
        ] {
            assert_eq!(
                transparent_routing_text("/v1/responses", &body),
                Some(String::new())
            );
        }
        let response = |id: &str| {
            transparent_routing_text(
                "/v1/responses",
                &serde_json::json!({
                    "prompt_cache_key":id, "client_metadata":{"request_id":id},
                    "input":[{"type":"message", "role":"user", "id":id,
                        "content":[{"type":"input_text", "text":"same prompt"}]}]
                }),
            )
        };
        assert_eq!(response("item_A"), response("item_B"));
    }
}
