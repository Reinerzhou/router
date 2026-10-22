# HTTP content affinity

The regular HTTP router derives worker affinity from the visible prompt without requiring a session ID. The routing key is used only for selection; the backend receives the original request and media.

| Endpoint | Key fields |
|---|---|
| `/v1/chat/completions` | Ordered tools and message roles, content, reasoning and tool calls/results |
| `/v1/responses` | Instructions, ordered tools and visible input messages/items |
| `/v1/messages` | System content, ordered tools, messages and nested content blocks |

Message boundaries, order, escaped text and media references are retained. Inline base64 media is hashed in the key. Sampling options, metadata and session identifiers are excluded.

The typed Chat path and transparent Responses/Messages paths forward end-to-end API headers. Incoming JSON framing and hop-by-hop headers are removed. A configured backend authorization key replaces caller authorization, and a Router-selected DP rank replaces a caller rank. The transparent path uses the default policy and the available worker pool.

Responses requests using `previous_response_id` or `item_reference` can refer to backend-held history absent from the request. The router has no response-ID-to-worker mapping. Send explicit history, commonly with `store=false`, when relying on stateless content affinity.
