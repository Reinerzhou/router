//! Tests that unknown/extra fields in ChatCompletionRequest are preserved through serde roundtrip.
use vllm_router_rs::protocols::spec::ChatCompletionRequest;

#[test]
fn test_extra_fields_preserved_on_deserialize() {
    let json = r#"{
        "model": "test-model",
        "messages": [{"role": "user", "content": "Hello"}],
        "max_tokens": 100,
        "return_token_ids": true,
        "some_custom_param": "value"
    }"#;

    let req: ChatCompletionRequest = serde_json::from_str(json).unwrap();

    assert!(
        req.other.contains_key("return_token_ids"),
        "return_token_ids should be captured in other"
    );
    assert!(
        req.other.contains_key("some_custom_param"),
        "some_custom_param should be captured in other"
    );
    assert_eq!(
        req.other["some_custom_param"],
        serde_json::Value::String("value".to_string())
    );
}

#[test]
fn test_extra_fields_survive_roundtrip() {
    let json = r#"{
        "model": "test-model",
        "messages": [{"role": "user", "content": "Hello"}],
        "max_tokens": 100,
        "return_token_ids": true
    }"#;

    let req: ChatCompletionRequest = serde_json::from_str(json).unwrap();
    let serialized = serde_json::to_string(&req).unwrap();
    let roundtrip: ChatCompletionRequest = serde_json::from_str(&serialized).unwrap();

    assert_eq!(roundtrip.other["return_token_ids"], serde_json::json!(true));
}

#[test]
fn test_extra_fields_appear_at_top_level_in_serialized_json() {
    let json = r#"{
        "model": "test-model",
        "messages": [{"role": "user", "content": "Hello"}],
        "return_token_ids": true
    }"#;

    let req: ChatCompletionRequest = serde_json::from_str(json).unwrap();
    let value: serde_json::Value = serde_json::to_value(&req).unwrap();

    assert!(
        value.get("return_token_ids").is_some(),
        "return_token_ids should be at the top level of the serialized JSON"
    );
    assert_eq!(value["return_token_ids"], serde_json::json!(true));
}

#[test]
fn test_no_other_fields_gives_empty_map() {
    let json = r#"{
        "model": "test-model",
        "messages": [{"role": "user", "content": "Hello"}]
    }"#;

    let req: ChatCompletionRequest = serde_json::from_str(json).unwrap();
    assert!(req.other.is_empty());
}

#[test]
fn test_lmdeploy_input_ids_is_typed_not_flattened() {
    let json = r#"{
        "model": "test-model",
        "messages": [],
        "input_ids": [1, 2, 3],
        "return_token_ids": true
    }"#;

    let req: ChatCompletionRequest = serde_json::from_str(json).unwrap();
    assert_eq!(req.input_ids, Some(vec![1, 2, 3]));
    assert!(!req.other.contains_key("input_ids"));
    assert_eq!(req.other["return_token_ids"], serde_json::json!(true));
}

#[test]
fn test_historical_reasoning_normalizes_to_lmdeploy_field() {
    use serde_json::json;

    for (fields, expected_reasoning) in [
        (json!({}), None),
        (
            json!({"reasoning": "vLLM reasoning"}),
            Some("vLLM reasoning"),
        ),
        (
            json!({"reasoning_content": "LMDeploy reasoning"}),
            Some("LMDeploy reasoning"),
        ),
        (
            json!({"reasoning": "vLLM reasoning", "reasoning_content": "LMDeploy reasoning"}),
            Some("LMDeploy reasoning"),
        ),
    ] {
        let mut assistant = json!({
            "role": "assistant",
            "content": "Let me check.",
            "tool_calls": [{
                "id": "call_1",
                "type": "function",
                "function": {"name": "terminal", "arguments": "{}"}
            }]
        });
        let mut expected_assistant = assistant.clone();
        if let Some(reasoning) = expected_reasoning {
            expected_assistant["reasoning_content"] = json!(reasoning);
        }
        assistant
            .as_object_mut()
            .unwrap()
            .extend(fields.as_object().unwrap().clone());
        let input = json!({
            "model": "test-model",
            "messages": [
                {"role": "user", "content": "Inspect the git diff."},
                assistant,
                {"role": "tool", "tool_call_id": "call_1", "content": "Done."}
            ],
            "chat_template_kwargs": {"clear_thinking": false}
        });

        let request: ChatCompletionRequest = serde_json::from_value(input.clone()).unwrap();
        let forwarded = serde_json::to_value(&request).unwrap();
        let mut expected_messages = input["messages"].clone();
        expected_messages[1] = expected_assistant;

        assert_eq!(forwarded["messages"], expected_messages, "fields: {fields}");
        assert_eq!(
            forwarded["chat_template_kwargs"],
            input["chat_template_kwargs"]
        );
    }
}

#[test]
fn test_reasoning_effort_values_survive_request_roundtrip() {
    use serde_json::json;

    for effort in ["low", "medium", "high", "max"] {
        let input = json!({
            "model": "test-model",
            "messages": [{"role": "user", "content": "Hi."}],
            "reasoning_effort": effort
        });
        let request: ChatCompletionRequest = serde_json::from_value(input).unwrap();
        let forwarded = serde_json::to_value(&request).unwrap();

        assert_eq!(forwarded["reasoning_effort"], effort);
    }
}
