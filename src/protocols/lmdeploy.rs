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
