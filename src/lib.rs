pub mod message;
#[cfg(feature = "lwt")]
pub mod availability_trait;
#[cfg(feature = "lwt")]
pub mod concrete;

#[cfg(feature = "validation")]
pub mod validation;

#[cfg(feature = "mock_client")]
pub mod mock;

use async_trait::async_trait;
#[cfg(feature = "lwt")]
use availability_trait::AvailabilityHelper;
pub use message::MqttMessage;
use std::fmt;
use tokio::sync::{broadcast, watch, oneshot};

/// Custom error type for MQTT pub/sub operations
#[derive(Debug, Clone)]
pub enum Mqtt5PubSubError {
    /// Subscription error
    SubscriptionError(String),
    /// Unsubscribe error
    UnsubscribeError(String),
    /// Publish error
    PublishError(String),
    /// Invalid topic
    InvalidTopic(String),
    /// Invalid QoS
    InvalidQoS(String),
    /// Timeout occurred
    TimeoutError(String),
    /// Other errors
    Other(String),
}

impl fmt::Display for Mqtt5PubSubError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Mqtt5PubSubError::SubscriptionError(msg) => write!(f, "Subscription error: {}", msg),
            Mqtt5PubSubError::UnsubscribeError(msg) => write!(f, "Unsubscribe error: {}", msg),
            Mqtt5PubSubError::PublishError(msg) => write!(f, "Publish error: {}", msg),
            Mqtt5PubSubError::InvalidTopic(msg) => write!(f, "Invalid topic: {}", msg),
            Mqtt5PubSubError::InvalidQoS(msg) => write!(f, "Invalid QoS: {}", msg),
            Mqtt5PubSubError::TimeoutError(msg) => write!(f, "Timeout error: {}", msg),
            Mqtt5PubSubError::Other(msg) => write!(f, "Error: {}", msg),
        }
    }
}

impl std::error::Error for Mqtt5PubSubError {}

/// Represents the success result of an MQTT publish operation
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MqttPublishSuccess {
    /// Message was sent (QoS 0)
    Sent,
    /// Message was acknowledged by broker (QoS 1, PUBACK received)
    Acknowledged,
    /// Message was fully completed (QoS 2, PUBCOMP received)
    Completed,
    /// Message was queued for sending (publish_nowait)
    Queued,
}

/// Represents the connection state of an MQTT client
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MqttConnectionState {
    /// Client is disconnected from the broker
    Disconnected,
    /// Client is in the process of connecting to the broker
    Connecting,
    /// Client is connected to the broker
    Connected,
    /// Client is in the process of disconnecting from the broker
    Disconnecting,
}

/// Trait defining the interface for MQTT pub/sub operations
/// 
/// This trait provides methods for publishing and subscribing to MQTT topics on an
/// already-connected MQTT client. Application code is responsible for managing the
/// client's connection lifecycle (connecting, disconnecting, reconnecting, etc.).
/// 
/// This trait is intended for use by libraries that need to publish or subscribe
/// to MQTT topics without needing to manage the underlying client connection.
#[async_trait]
pub trait Mqtt5PubSub {
    /// Get the client ID
    fn get_client_id(&self) -> String;

    /// Get a receiver for monitoring the client's connection state
    /// 
    /// The implementation must send a new state value to the watch channel whenever the
    /// connection state changes (e.g., from `Connecting` to `Connected`, or from `Connected`
    /// to `Disconnected`).
    fn get_state(&self) -> watch::Receiver<MqttConnectionState>;

    /// Subscribe to a topic with the specified QoS level
    /// 
    /// This function awaits until a SUBACK is received from the broker before returning.
    /// Messages received on this subscription will be sent to the provided channel.
    /// Returns a subscription identifier that will be set in the `subscription_id` field
    /// of all `MqttMessage`s received on this subscription.
    async fn subscribe(&mut self, topic: String, qos: message::QoS, tx: broadcast::Sender<MqttMessage>) -> Result<u32, Mqtt5PubSubError>;

    /// Unsubscribe from a topic
    /// 
    /// This function awaits until an UNSUBACK is received from the broker before returning.
    /// The topic must be identical to the one used with the `subscribe()` method.
    async fn unsubscribe(&mut self, topic: String) -> Result<(), Mqtt5PubSubError>;

    /// Publish a message to the broker (awaits completion)
    /// 
    /// The function blocks according to the QoS level set in the message:
    /// - QoS 0: Blocks until the message is sent
    /// - QoS 1: Blocks until a PUBACK is received from the broker
    /// - QoS 2: Blocks until a PUBCOMP is received from the broker
    async fn publish(&mut self, message: MqttMessage) -> Result<MqttPublishSuccess, Mqtt5PubSubError>;

    /// Publish a message to the broker and returns a oneshot channel that receives when done.
    /// 
    /// The oneshot channel will receive when the publish is complete according to the QoS level set in the message:
    /// - QoS 0: Blocks until the message is sent
    /// - QoS 1: Blocks until a PUBACK is received from the broker
    /// - QoS 2: Blocks until a PUBCOMP is received from the broker
    async fn publish_noblock(&mut self, message: MqttMessage) -> oneshot::Receiver<Result<MqttPublishSuccess, Mqtt5PubSubError>>;

    /// Publish a message without waiting for completion (fire and forget)
    /// 
    /// This function returns as soon as the message is queued to be sent, without waiting
    /// for any acknowledgment from the broker.
    fn publish_nowait(&mut self, message: MqttMessage) -> Result<MqttPublishSuccess, Mqtt5PubSubError>;

    /// Get an AvailabilityHelper for publishing availability messages.
    ///
    /// Returns `Some(Box<dyn AvailabilityHelper>)` when the client can provide
    /// an availability helper, or `None` when not available.
    #[cfg(feature = "lwt")]
    fn get_availability_helper(&mut self) -> Option<Box<dyn AvailabilityHelper>>;
}

// Re-export commonly used types
pub use message::{MqttMessageBuilder, QoS};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mqtt_pubsub_error_display() {
        let err = Mqtt5PubSubError::SubscriptionError("invalid topic".to_string());
        assert_eq!(err.to_string(), "Subscription error: invalid topic");

        let err = Mqtt5PubSubError::UnsubscribeError("not subscribed".to_string());
        assert_eq!(err.to_string(), "Unsubscribe error: not subscribed");

        let err = Mqtt5PubSubError::PublishError("publish failed".to_string());
        assert_eq!(err.to_string(), "Publish error: publish failed");

        let err = Mqtt5PubSubError::InvalidTopic("bad topic".to_string());
        assert_eq!(err.to_string(), "Invalid topic: bad topic");

        let err = Mqtt5PubSubError::InvalidQoS("bad qos".to_string());
        assert_eq!(err.to_string(), "Invalid QoS: bad qos");

        let err = Mqtt5PubSubError::Other("something went wrong".to_string());
        assert_eq!(err.to_string(), "Error: something went wrong");
    }

    #[test]
    fn test_mqtt_pubsub_error_is_error_trait() {
        let err = Mqtt5PubSubError::PublishError("test".to_string());
        // Verify it implements std::error::Error
        let _: &dyn std::error::Error = &err;
    }

    #[test]
    fn test_mqtt_pubsub_error_clone() {
        let err1 = Mqtt5PubSubError::PublishError("test error".to_string());
        let err2 = err1.clone();
        assert_eq!(err1.to_string(), err2.to_string());
    }

    // Mock implementation for testing trait signature
    struct MockPubSubClient {
        client_id: String,
        state_rx: watch::Receiver<MqttConnectionState>,
    }

    impl MockPubSubClient {
        fn new() -> Self {
            let (tx, rx) = watch::channel(MqttConnectionState::Connected);
            drop(tx); // Drop the sender as we don't need it in the mock
            Self {
                client_id: "test-client".to_string(),
                state_rx: rx,
            }
        }
    }

    #[async_trait]
    impl Mqtt5PubSub for MockPubSubClient {
        fn get_client_id(&self) -> String {
            self.client_id.clone()
        }

        fn get_state(&self) -> watch::Receiver<MqttConnectionState> {
            self.state_rx.clone()
        }

        async fn subscribe(&mut self, _topic: String, _qos: message::QoS, _tx: broadcast::Sender<MqttMessage>) -> Result<u32, Mqtt5PubSubError> {
            Ok(1)
        }

        async fn unsubscribe(&mut self, _topic: String) -> Result<(), Mqtt5PubSubError> {
            Ok(())
        }

        async fn publish(&mut self, _message: MqttMessage) -> Result<MqttPublishSuccess, Mqtt5PubSubError> {
            Ok(MqttPublishSuccess::Sent)
        }

        async fn publish_noblock(&mut self, _message: MqttMessage) -> oneshot::Receiver<Result<MqttPublishSuccess, Mqtt5PubSubError>> {
            let (tx, rx) = oneshot::channel();
            let _ = tx.send(Ok(MqttPublishSuccess::Sent));
            rx
        }

        fn publish_nowait(&mut self, _message: MqttMessage) -> Result<MqttPublishSuccess, Mqtt5PubSubError> {
            Ok(MqttPublishSuccess::Queued)
        }

        #[cfg(feature = "lwt")]
        fn get_availability_helper(&mut self) -> Option<Box<dyn crate::availability_trait::AvailabilityHelper>> {
            Some(Box::new(crate::concrete::GenericAvailability::new(self.client_id.clone())))
        }
    }

    #[tokio::test]
    async fn test_mock_pubsub_subscribe() {
        let (tx, _rx) = broadcast::channel(10);
        let mut client = MockPubSubClient::new();
        let result = client.subscribe("test/topic".to_string(), message::QoS::AtLeastOnce, tx).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 1);
    }

    #[tokio::test]
    async fn test_mock_pubsub_publish() {
        let mut client = MockPubSubClient::new();
        let msg = MqttMessage::simple(
            "test/topic".to_string(),
            message::QoS::AtMostOnce,
            false,
            bytes::Bytes::from("test"),
        );
        let result = client.publish(msg).await;
        assert!(result.is_ok());
    }

    #[test]
    fn test_mock_pubsub_publish_nowait() {
        let mut client = MockPubSubClient::new();
        let msg = MqttMessage::simple(
            "test/topic".to_string(),
            message::QoS::AtMostOnce,
            false,
            bytes::Bytes::from("test"),
        );
        let result = client.publish_nowait(msg);
        assert!(result.is_ok());
    }
}
