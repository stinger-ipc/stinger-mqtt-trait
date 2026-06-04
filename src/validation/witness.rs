//! Witness client for verifying messages published to the broker
//!
//! This module provides a wrapper around `rumqttc::AsyncClient` that can subscribe to topics
//! and verify that messages are actually being published to the broker.
//!
//! # Example
//!
//! ```ignore
//! use stinger_mqtt_trait::validation::broker::BrokerTransport;
//! use stinger_mqtt_trait::validation::witness::WitnessClient;
//! use bytes::Bytes;
//!
//! #[tokio::main]
//! async fn main() {
//!     // Create witness client
//!     let transport = BrokerTransport::Tcp {
//!         host: "localhost".to_string(),
//!         port: 1883,
//!     };
//!     let witness = WitnessClient::new(transport);
//!
//!     // Subscribe to a topic
//!     witness.subscribe("test/topic", message::QoS::AtLeastOnce).await.unwrap();
//!
//!     // Wait for messages
//!     let messages = witness.wait_for_messages("test/topic", 1, Duration::from_secs(5)).await.unwrap();
//!     println!("Received {} messages", messages.len());
//! }
//! ```

use crate::message;
use crate::validation::broker::BrokerTransport;
use bytes::Bytes;
use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS as RumqttcQoS};
use std::sync::{Arc, Mutex};
use tokio::time::{timeout, Duration};

/// A witness client that can subscribe to topics and capture published messages
/// for verification purposes
pub struct WitnessClient {
    client: AsyncClient,
    received_messages: Arc<Mutex<Vec<CapturedMessage>>>,
    subscribed_topics: Arc<Mutex<Vec<String>>>,
}

/// A captured message with all its details
#[derive(Debug, Clone)]
pub struct CapturedMessage {
    pub topic: String,
    pub payload: Bytes,
    pub qos: message::QoS,
    pub retain: bool,
    pub timestamp: std::time::Instant,
}

impl WitnessClient {
    /// Create a new witness client from a BrokerTransport
    ///
    /// This will create a rumqttc client internally and start the event loop
    /// in a background task to capture messages.
    pub fn new(transport: BrokerTransport) -> Self {
        // Generate a unique client ID
        let client_id = format!(
            "witness_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );

        // Create MQTT options based on transport type
        let mut mqtt_options = match &transport {
            BrokerTransport::Tcp { host, port } => {
                MqttOptions::new(client_id, rumqttc::Broker::tcp(host.clone(), *port))
            }
            BrokerTransport::Unix { path } => {
                MqttOptions::new(client_id, rumqttc::Broker::unix(path.clone()))
            }
        };

        // Set connection parameters
        mqtt_options.set_keep_alive(5);

        // Create client and event loop
        let (client, mut eventloop) = AsyncClient::new(mqtt_options, 10);

        let received_messages = Arc::new(Mutex::new(Vec::new()));
        let received_clone = received_messages.clone();
        
        // Spawn task to handle incoming events
        tokio::spawn(async move {
            loop {
                match eventloop.poll().await {
                    Ok(Event::Incoming(Packet::Publish(publish))) => {
                        let qos = match publish.qos {
                            RumqttcQoS::AtMostOnce => message::QoS::AtMostOnce,
                            RumqttcQoS::AtLeastOnce => message::QoS::AtLeastOnce,
                            RumqttcQoS::ExactlyOnce => message::QoS::ExactlyOnce,
                        };
                        
                        let captured = CapturedMessage {
                            topic: String::from_utf8_lossy(&publish.topic).to_string(),
                            payload: Bytes::from(publish.payload.to_vec()),
                            qos,
                            retain: publish.retain,
                            timestamp: std::time::Instant::now(),
                        };
                        
                        received_clone.lock().unwrap().push(captured);
                    }
                    Err(e) => {
                        eprintln!("Witness client event loop error: {:?}", e);
                        break;
                    }
                    _ => {}
                }
            }
        });
        
        Self {
            client,
            received_messages,
            subscribed_topics: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Get a reference to the underlying AsyncClient
    pub fn client(&self) -> &AsyncClient {
        &self.client
    }

    /// Subscribe to a topic
    pub async fn subscribe(
        &self,
        topic: impl Into<String>,
        qos: message::QoS,
    ) -> Result<(), String> {
        let topic = topic.into();
        
        let rumqttc_qos = match qos {
            message::QoS::AtMostOnce => RumqttcQoS::AtMostOnce,
            message::QoS::AtLeastOnce => RumqttcQoS::AtLeastOnce,
            message::QoS::ExactlyOnce => RumqttcQoS::ExactlyOnce,
        };
        
        self.client
            .subscribe(&topic, rumqttc_qos)
            .await
            .map_err(|e| format!("Failed to subscribe witness: {}", e))?;

        // Track subscription
        self.subscribed_topics.lock().unwrap().push(topic);

        Ok(())
    }

    /// Get all captured messages
    pub fn get_messages(&self) -> Vec<CapturedMessage> {
        self.received_messages.lock().unwrap().clone()
    }

    /// Get messages for a specific topic
    pub fn get_messages_for_topic(&self, topic: &str) -> Vec<CapturedMessage> {
        self.received_messages
            .lock()
            .unwrap()
            .iter()
            .filter(|msg| msg.topic == topic)
            .cloned()
            .collect()
    }

    /// Wait for a specific number of messages (total across all topics)
    pub async fn wait_for_message_count(
        &self,
        count: usize,
        timeout_duration: Duration,
    ) -> Result<Vec<CapturedMessage>, String> {
        let start = std::time::Instant::now();
        
        loop {
            let messages = self.get_messages();
            if messages.len() >= count {
                return Ok(messages);
            }

            if start.elapsed() > timeout_duration {
                return Err(format!(
                    "Timeout waiting for {} messages. Got {} messages",
                    count,
                    messages.len()
                ));
            }

            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// Wait for a specific number of messages on a topic
    pub async fn wait_for_messages(
        &self,
        topic: &str,
        count: usize,
        timeout_duration: Duration,
    ) -> Result<Vec<CapturedMessage>, String> {
        let start = std::time::Instant::now();
        
        loop {
            let messages = self.get_messages_for_topic(topic);
            if messages.len() >= count {
                return Ok(messages);
            }

            if start.elapsed() > timeout_duration {
                return Err(format!(
                    "Timeout waiting for {} messages on '{}'. Got {} messages",
                    count,
                    topic,
                    messages.len()
                ));
            }

            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// Wait for a message with specific payload
    pub async fn wait_for_payload(
        &self,
        topic: &str,
        expected_payload: &[u8],
        timeout_duration: Duration,
    ) -> Result<CapturedMessage, String> {
        let start = std::time::Instant::now();
        
        loop {
            let messages = self.get_messages_for_topic(topic);
            if let Some(msg) = messages.iter().find(|m| m.payload.as_ref() == expected_payload) {
                return Ok(msg.clone());
            }

            if start.elapsed() > timeout_duration {
                return Err(format!(
                    "Timeout waiting for payload on '{}'. Received {} messages",
                    topic,
                    messages.len()
                ));
            }

            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// Clear all captured messages
    pub fn clear(&self) {
        self.received_messages.lock().unwrap().clear();
    }

    /// Get count of captured messages
    pub fn message_count(&self) -> usize {
        self.received_messages.lock().unwrap().len()
    }

    /// Verify a message was received on a topic
    pub fn verify_message_received(
        &self,
        topic: &str,
        expected_payload: &[u8],
    ) -> Result<(), String> {
        let messages = self.get_messages_for_topic(topic);
        
        if messages.is_empty() {
            return Err(format!("No messages received on topic '{}'", topic));
        }

        for msg in &messages {
            if msg.payload.as_ref() == expected_payload {
                return Ok(());
            }
        }

        Err(format!(
            "Expected payload not found on '{}'. Received {} messages",
            topic,
            messages.len()
        ))
    }
}



/// Helper function to verify a publish operation using rumqttc client
///
/// This publishes a message and verifies it was received by a witness client
pub async fn verify_publish(
    publisher: &AsyncClient,
    witness: &WitnessClient,
    topic: &str,
    payload: Bytes,
    qos: message::QoS,
) -> Result<(), String> {
    // Clear previous messages
    witness.clear();

    // Convert QoS
    let rumqttc_qos = match qos {
        message::QoS::AtMostOnce => RumqttcQoS::AtMostOnce,
        message::QoS::AtLeastOnce => RumqttcQoS::AtLeastOnce,
        message::QoS::ExactlyOnce => RumqttcQoS::ExactlyOnce,
    };

    // Publish
    publisher
        .publish(topic, rumqttc_qos, false, payload.clone())
        .await
        .map_err(|e| format!("Failed to publish: {}", e))?;

    // Wait for message
    let result = timeout(
        Duration::from_secs(5),
        witness.wait_for_payload(topic, &payload, Duration::from_secs(5)),
    )
    .await
    .map_err(|_| "Timeout waiting for message verification".to_string())?;

    result.map(|_| ())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_captured_message_debug() {
        let msg = CapturedMessage {
            topic: "test/topic".to_string(),
            payload: Bytes::from("test"),
            qos: message::QoS::AtMostOnce,
            retain: false,
            timestamp: std::time::Instant::now(),
        };
        
        let debug_str = format!("{:?}", msg);
        assert!(debug_str.contains("test/topic"));
    }
}
