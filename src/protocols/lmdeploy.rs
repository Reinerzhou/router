//! Backend-specific historical reasoning wire format.
use serde_json::Value;
pub(crate) fn outbound(mut body: Value, enabled: bool) -> Value {
    if enabled {
        if let Some(messages) = body.get_mut("messages").and_then(Value::as_array_mut) {
            for message in messages {
                if message.get("role").and_then(Value::as_str) == Some("assistant") {
                    if let Some(fields) = message.as_object_mut() {
                        if let Some(reasoning) = fields.remove("reasoning") {
                            fields.insert("reasoning_content".into(), reasoning);
                        }
                    }
                }
            }
        }
    }
    body
}

pub(crate) fn inbound(mut body: Value, enabled: bool) -> Value {
    if enabled {
        if let Some(messages) = body.get_mut("messages").and_then(Value::as_array_mut) {
            for message in messages {
                if message.get("role").and_then(Value::as_str) == Some("assistant") {
                    if let Some(reasoning) = message
                        .get("reasoning_content")
                        .and_then(Value::as_str)
                        .map(str::to_owned)
                    {
                        message["reasoning"] = Value::String(reasoning);
                    }
                }
            }
        }
    }
    body
}

#[cfg(all(test, feature = "cpu", feature = "ready"))]
mod tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn backend_precedence_and_empty_reasoning() {
        for value in ["", "LMDeploy"] {
            let original = json!({"messages":[{"role":"assistant","content":"answer","reasoning":"vLLM","reasoning_content":value}]});
            let body: crate::protocols::spec::ChatCompletionRequest =
                serde_json::from_value(inbound(original.clone(), true)).unwrap();
            let forwarded = outbound(serde_json::to_value(body).unwrap(), true);
            assert_eq!(forwarded["messages"][0]["reasoning_content"], value);
            assert!(forwarded["messages"][0].get("reasoning").is_none());
            assert_eq!(inbound(original.clone(), false), original);
        }
    }
}
