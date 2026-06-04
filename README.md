# stinger-mqtt-trait

A Rust **minimal trait abstraction** for MQTT pub/sub operations, providing a clean interface for publishing and subscribing to MQTT topics without being tied to a specific MQTT library. It allows libraries to perform MQTT pub/sub operations using a client connection managed by the application.

It is part of the "Stinger" suite of inter-process communication tools, but has no requirements on the suite and can easily be used elsewhere. [See documentation](https://docs.rs/stinger-mqtt-trait/latest/stinger_mqtt_trait/)

## Design Philosophy

**stinger-mqtt-trait is intentionally minimal and focused on pub/sub operations.** The trait defines the essential pub/sub operations that MQTT clients must support, while deliberately avoiding connection lifecycle management and implementation details that would constrain implementers.

### Core Principle: Separation of Concerns

**Application manages connections, libraries use pub/sub:**
- **Application code** is responsible for managing the MQTT client connection lifecycle (connecting, disconnecting, reconnecting, etc.)
- **Library code** implements `Mqtt5PubSub` to provide pub/sub operations on an already-connected client
- This separation allows libraries to focus on their domain logic while the application handles connection management

### Why Minimal and Pub/Sub Focused?

- **Maximum Flexibility**: Implementers can use any underlying MQTT library (rumqttc, paho-mqtt, custom embedded clients, etc.)
- **No Lock-in**: Libraries accepting `Mqtt5PubSub` work with any implementation - swap MQTT backends without changing application code
- **Implementation Freedom**: Rich features (metrics, compression, batching, RPC patterns, connection management) belong in concrete implementations or application code, not the trait
- **Testability**: The minimal surface area makes mocking straightforward (see `mock_client` feature)
- **Clear Boundaries**: Connection management is application-specific; pub/sub operations are library-level concerns

### What This Trait Is NOT

This is **not** a batteries-included MQTT framework or full client abstraction. If you need:

- Connection lifecycle management (connect, disconnect, reconnect)
- Extensive configuration options (connection timeouts, keep-alive tuning, TLS settings, etc.)
- Advanced features (automatic reconnection, circuit breakers, compression, metrics)
- Convenience helpers (JSON publish/subscribe, RPC patterns)
- Optimized batch operations

...these belong in **your concrete implementation** or application code. The trait simply ensures your implementation's pub/sub operations can be used anywhere `Mqtt5PubSub` is accepted.

## Features

- **Trait-based pub/sub abstraction** - Libraries implement `Mqtt5PubSub` trait with any MQTT client
- **MQTT 5.0 ready** - `MqttMessage` struct supports MQTT v5 properties (content type, correlation data, response topic, user properties)
- **Flexible publish semantics** - Three distinct methods for different use cases:
  - `publish()` - Await until broker acknowledges (QoS-aware: sent/PUBACK/PUBCOMP)
  - `publish_nowait()` - Fire-and-forget, returns immediately after queuing (synchronous, no async overhead)
  - `publish_noblock()` - Returns oneshot receiver for concurrent operations
- **Channel-based subscriptions** - User-provided `broadcast::Sender` enables flexible message routing:
  - Multiple concurrent handlers via broadcast fan-out
  - Channels are more idiomatic in async Rust code, support `Sync + Send`, and make it much easier to use with context than callbacks which must have `'static` lifetimes
- **Connection state monitoring** - `watch::Receiver` for tracking connection lifecycle (read-only from library perspective)
- **Client identification** - `get_client_id()` method for identifying the MQTT client instance
- **Builder pattern for messages** - Ergonomic construction with compile-time field validation via `MqttMessageBuilder`
- **Mock client** - Stateless mock implementation for testing (feature: `mock_client`)
- **Validation suite** - Optional test suite for implementation verification (feature: `validation`)
 - **Availability helper** - Optional `AvailabilityHelper` trait and a concrete `GenericAvailability` implementation for publishing/consuming LWT-style availability messages

## Design Decisions & Rationale

### Why Pub/Sub Only (No Connection Management)?

Connection lifecycle varies dramatically between MQTT implementations and deployment scenarios:

- Some clients auto-reconnect, others require manual reconnection
- TLS configuration, authentication methods, and connection options are implementation-specific
- Embedded vs. cloud vs. local broker scenarios have different requirements
- Connection pooling, failover, and high-availability patterns are application concerns

**The solution**: Applications create and connect their MQTT client implementation, then pass it to libraries that use `Mqtt5PubSub` for pub/sub operations. This separation of concerns provides maximum flexibility while maintaining trait compatibility.

### Why `broadcast::Sender` for Subscriptions?

The `subscribe()` method requires a `broadcast::Sender<MqttMessage>` parameter. While other channel types are viable options, `tokio::sync::broadcast` was selected because of the wide support for `tokio` and ability to support multiple consumers. This design choice maximizes flexibility:

**Application controls the channel**: You create the broadcast channel with your desired capacity and lagging strategy, then pass the sender to `subscribe()`. This means:

- **Fan-out**: Clone receivers for multiple independent handlers
- **Adaptable**: Wrap with your preferred pattern (callbacks, mpsc conversion, custom routing)
- **Explicit**: Buffer sizes and overflow behavior are in your control, not hidden in the trait

### Why Three Publish Methods?

Each method serves a **semantically distinct** use case:

- **`publish()`** - "I need confirmation" - Most common case, blocks until QoS acknowledgment received
- **`publish_noblock()`** - "I need to do other work" - Returns immediately with oneshot receiver for concurrent workflows
- **`publish_nowait()`** - "Fire and forget" - **Synchronous** method with no async overhead, ideal for high-frequency telemetry or non-critical messages

These are not redundant - they represent fundamentally different control flow patterns and have distinctly different return types and semantics.

## Common Misconceptions

### "The trait should include connection methods"

**Why not**: Connection management is inherently application-specific and implementation-dependent. Different MQTT libraries have completely different connection APIs, configuration options, and lifecycle models. The current design recognizes this by:

- **Separating concerns**: Applications handle connections, libraries handle pub/sub
- **Maximizing flexibility**: Implementations can use any connection strategy
- **Avoiding false abstraction**: A unified connection API would either be too minimal or too constraining

**The solution**: Applications create and manage their MQTT client (with whatever connection logic they need), then use it via the `Mqtt5PubSub` trait.

### "Subscriptions should support callbacks, not channels"

**Why channels are better for a trait**:

- **More flexible**: Broadcast channels can be converted to callbacks, but callbacks can't easily be converted to channels
- **Decoupled**: The trait doesn't dictate concurrency model - you spawn handlers however you want
- **Multiple consumers**: Broadcast allows fan-out; callbacks typically don't
- **Standard library**: Uses tokio's well-tested primitives rather than defining custom callback traits

If you prefer callbacks, write an adapter function.

### "The trait is missing essential features like metrics/batching/reconnection"

**Why they're not in the trait**:

- **Not universal**: Embedded clients may not have metrics; some protocols don't support batching efficiently
- **Implementation detail**: How you implement auto-reconnection varies wildly between clients and is an application concern
- **Optional**: Many use cases don't need these features
- **Pub/sub focus**: The trait specifically avoids connection lifecycle concerns

**Where they belong**: In your concrete implementation or as extension traits. The base trait ensures interoperability; implementations provide richness.

## When to Use This Trait

### ✅ Use stinger-mqtt-trait when

- **Writing library code** that needs MQTT pub/sub but shouldn't force a specific client implementation or manage connections
- **Building modular systems** where different components might use different MQTT backends
- **Testing** - swap real MQTT clients with mocks without changing code
- **Long-term maintenance** - avoid coupling to a specific MQTT library that might become unmaintained
- **Embedded systems** - implement the trait with minimal, specialized MQTT clients

### ❌ Don't use this trait when

- You're building an end application that will only ever use one MQTT client and directly manages it - just use that client's API directly
- You're working in a constrained environment where trait object overhead matters
- You need tight coupling between connection management and pub/sub operations

## Usage

Add this to your `Cargo.toml`:

```toml
[dependencies]
stinger-mqtt-trait = "0.2"
```

To use the validation suite for testing your implementation:

Note: Implementations may optionally provide an availability helper. The trait method
`get_availability_helper(&mut self) -> Option<Box<dyn AvailabilityHelper>>` returns
`Some(...)` when an availability helper is available or `None` when not. Consumers
should handle the `Option` accordingly.

## Availability Helper

The crate provides an `AvailabilityHelper` abstraction for publishing and interpreting
client availability (online/offline) messages. A lightweight concrete implementation,
`GenericAvailability`, is included for common cases:

- Topic: `system/{client_id}`
- Payload: JSON object `{"online": true}` / `{"online": false}`
- QoS: `AtLeastOnce` (1)
- Retain: `true`
- Republish interval: `None` (no automatic republish)

`AvailabilityHelper` also exposes a JSONPath to locate the boolean `online` field
in availability payloads; the crate uses `jsonpath-rust` to match that value when
decoding incoming availability messages. The `GenericAvailability` implementation
uses the JSONPath `$.online`.

The mock client and test helpers return `Some(Box::new(GenericAvailability::new(client_id)))`
so tests and examples exercise availability handling by default.

## Validation Suite
[dev-dependencies]
stinger-mqtt-trait = { version = "0.2", features = ["validation"] }
```

### Creating Messages

```rust
use stinger_mqtt_trait::{MqttMessage, MqttMessageBuilder, QoS};

let msg = MqttMessageBuilder::default()
    .topic("sensor/data")
    .qos(QoS::ExactlyOnce)
    .retain(false)
    .payload(Payload::from_serializable(&my_struct)?)
    .content_type(Some("application/json".to_string()))
    .response_topic(Some("sensor/response".to_string()))
    .user_property("PropertyKey", "PropertyValue")
    .build()?;
```

Each consuming library will have their own style of messages, and I recommend the libraries simplify the builder pattern with static functions to create messages like this:

```rust
pub fn request_message<T: Serialize>(topic: &str, payload: &T, correlation_id: Uuid, response_topic: String) -> Result<MqttMessage> {
    MqttMessageBuilder::default()
        .topic(topic)
        .object_payload(payload)?
        .qos(QoS::ExactlyOnce)
        .retain(false)
        .correlation_data(Bytes::from(correlation_id.to_string()))
        .response_topic(response_topic)
        .build()?
}
```

### Implementing the Trait

```rust
use stinger_mqtt_trait::{Mqtt5PubSub, MqttMessage, Mqtt5PubSubError, MqttConnectionState, MqttPublishSuccess, QoS};
use async_trait::async_trait;
use tokio::sync::{broadcast, oneshot, watch};

struct MyMqtt5PubSubClient {
    // Your implementation fields (wrapping an actual MQTT client)
    // Note: Your client should already be connected before being used via this trait
}

#[async_trait]
impl Mqtt5PubSub for MyMqtt5PubSubClient {
    fn get_client_id(&self) -> String {
        // Return the MQTT client ID
        todo!()
    }

    fn get_state(&self) -> watch::Receiver<MqttConnectionState> {
        // Return watch receiver for connection state monitoring
        todo!()
    }

    async fn subscribe(
        &mut self,
        topic: String,
        qos: QoS,
        tx: broadcast::Sender<MqttMessage>
    ) -> Result<u32, Mqtt5PubSubError> {
        // Your subscribe logic, return subscription ID
        todo!()
    }

    async fn unsubscribe(&mut self, topic: String) -> Result<(), Mqtt5PubSubError> {
        // Your unsubscribe logic
        todo!()
    }

    async fn publish(&mut self, message: MqttMessage) -> Result<MqttPublishSuccess, Mqtt5PubSubError> {
        // Your publish logic (awaits completion based on QoS)
        todo!()
    }

    async fn publish_noblock(&mut self, message: MqttMessage) 
        -> oneshot::Receiver<Result<MqttPublishSuccess, Mqtt5PubSubError>> {
        // Your non-blocking publish logic (returns immediately with oneshot receiver)
        todo!()
    }

    fn publish_nowait(&mut self, message: MqttMessage) -> Result<MqttPublishSuccess, Mqtt5PubSubError> {
        // Your fire-and-forget publish logic (synchronous, no async overhead)
        todo!()
    }
}
```

## Validation Suite

The `validation` feature provides a comprehensive test suite for validating `Mqtt5PubSub` implementations:

```rust
use stinger_mqtt_trait::validation::run_full_pubsub_validation_suite;

#[tokio::test]
async fn test_my_implementation() {
    // Application connects the client first
    let mut client = MyMqtt5PubSubClient::new();
    // ... connect your client here ...
    
    // Test pub/sub operations
    run_full_pubsub_validation_suite(&mut client)
        .await
        .unwrap();
}
```

See `src/validation/README.md` for more details on the validation suite.

## FAQ

### Q: Why doesn't the trait return the granted QoS from subscribe?

**A**: This is a valid point - MQTT brokers can grant a different QoS than requested.  A future version could return both the subscription ID and granted QoS, and this is an area for improvement.

### Q: Can I add my own methods to implementations?

**A**: Absolutely! The trait defines the minimum interface. Your implementation can (and should) add:

- Rich configuration in constructors
- Convenience methods (e.g., `publish_json`, `subscribe_many`)
- Advanced features (metrics, reconnection policies, etc.)
- Domain-specific helpers

### Q: What about MQTT v3.1.1 vs v5.0?

**A**: The trait is designed to work with both:

- `MqttMessage` includes v5.0 properties (optional to use)
- Implementations can support either protocol version
- V3.1.1 clients simply ignore v5 fields, although this isn't recommended without making it abundantly clear to the application developer (would break the swap-ability that using a trait provides)

### Q: How do I manage connection lifecycle?

**A**: Connection management is intentionally outside the scope of the `Mqtt5PubSub` trait. Your application should:

1. Create and configure your MQTT client implementation (with whatever connection settings you need)
2. Connect the client to the broker using your implementation's methods
3. Pass the connected client to libraries that use `Mqtt5PubSub` for pub/sub operations

This separation allows maximum flexibility in how you handle connections, reconnections, and lifecycle management.

## License

MIT


