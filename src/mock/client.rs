//! Mock MQTT5 pub/sub client for testing purposes

use crate::{Mqtt5PubSub, MqttConnectionState, Mqtt5PubSubError, MqttPublishSuccess, message::{MqttMessage, QoS}};
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::sync::{broadcast, watch};

/// A stateless mock MQTT pub/sub client for testing purposes
/// 
/// This implementation stores published messages and allows simulating received messages
/// through registered subscriptions. It's designed for unit testing MQTT pub/sub operations
/// without requiring an actual MQTT broker or managing connection lifecycle.
/// 
/// The mock client assumes the connection is always available and ready.
#[derive(Clone)]
pub struct MockClient {
    client_id: String,
    state_rx: watch::Receiver<MqttConnectionState>,
    /// Storage for published messages
    published_messages: Arc<Mutex<Vec<MqttMessage>>>,
    /// Storage for subscription senders and their IDs
    subscriptions: Arc<Mutex<Vec<(String, u32, broadcast::Sender<MqttMessage>)>>>,
    /// Next subscription ID to assign
    next_sub_id: Arc<Mutex<u32>>,
    /// Storage for retained messages, keyed by topic
    retained_messages: Arc<Mutex<HashMap<String, MqttMessage>>>,
}

impl MockClient {
    /// Create a new MockClient with the given client ID
    pub fn new(client_id: impl Into<String>) -> Self {
        let (state_tx, state_rx) = watch::channel(MqttConnectionState::Connected);
        drop(state_tx); // Stateless - always shows as connected
        Self {
            client_id: client_id.into(),
            state_rx,
            published_messages: Arc::new(Mutex::new(Vec::new())),
            subscriptions: Arc::new(Mutex::new(Vec::new())),
            next_sub_id: Arc::new(Mutex::new(1)),
            retained_messages: Arc::new(Mutex::new(HashMap::new())),
        }
    }
    
    /// Create a new MockClient with a default client ID
    pub fn new_default() -> Self {
        Self::new("mock-client")
    }

    /// Get the last published message, if any
    pub fn last_published_message(&self) -> Option<MqttMessage> {
        self.published_messages.lock().unwrap().last().cloned()
    }

    /// Get all published messages
    pub fn published_messages(&self) -> Vec<MqttMessage> {
        self.published_messages.lock().unwrap().clone()
    }

    /// Clear all published messages
    pub fn clear_published_messages(&mut self) {
        self.published_messages.lock().unwrap().clear();
    }

    /// Simulate receiving a message on a subscribed topic
    /// 
    /// This will send the message through any registered subscription senders
    /// that match the given topic. Returns the number of subscriptions that
    /// received the message.
    ///
    /// If the message has the `retain` flag set, it will be stored (or replaced)
    /// in the retained messages store for that topic. Retained messages are
    /// replayed to new subscribers when they call `subscribe()`.
    pub fn simulate_receive(&self, mut message: MqttMessage) -> Result<usize, Mqtt5PubSubError> {
        // Store retained messages
        if message.retain {
            let mut retained = self.retained_messages.lock().unwrap();
            retained.insert(message.topic.clone(), message.clone());
        }

        let subscriptions = self.subscriptions.lock().unwrap();
        let mut sent_count = 0;

        for (topic, sub_id, tx) in subscriptions.iter() {
            if Self::topic_matches(topic, &message.topic) {
                message.subscription_id = Some(*sub_id);
                tx.send(message.clone())
                    .map_err(|e| Mqtt5PubSubError::Other(format!("Failed to send message to subscription: {}", e)))?;
                sent_count += 1;
            }
        }

        Ok(sent_count)
    }

    /// Get all retained messages
    pub fn retained_messages(&self) -> HashMap<String, MqttMessage> {
        self.retained_messages.lock().unwrap().clone()
    }

    /// Clear all retained messages
    pub fn clear_retained_messages(&mut self) {
        self.retained_messages.lock().unwrap().clear();
    }

    /// Returns `true` if `topic` matches the MQTT subscription `filter`.
    ///
    /// Supports MQTT wildcard characters:
    /// - `+` matches exactly one topic level (e.g. `sensor/+/temp` matches `sensor/room1/temp`)
    /// - `#` matches zero or more topic levels and must appear as the last character
    ///   (e.g. `sensor/#` matches `sensor`, `sensor/room1`, `sensor/room1/temp`)
    pub fn topic_matches(filter: &str, topic: &str) -> bool {
        let mut filter_parts = filter.split('/');
        let mut topic_parts = topic.split('/');

        loop {
            match filter_parts.next() {
                Some("#") => return true,
                Some("+") => {
                    if topic_parts.next().is_none() {
                        return false;
                    }
                }
                Some(f) => match topic_parts.next() {
                    Some(t) if t == f => {}
                    _ => return false,
                },
                None => return topic_parts.next().is_none(),
            }
        }
    }
}

impl Default for MockClient {
    fn default() -> Self {
        Self::new_default()
    }
}

#[async_trait]
impl Mqtt5PubSub for MockClient {
    fn get_client_id(&self) -> String {
        self.client_id.clone()
    }

    fn get_state(&self) -> watch::Receiver<MqttConnectionState> {
        self.state_rx.clone()
    }

    async fn subscribe(
        &mut self,
        topic: String,
        _qos: QoS,
        tx: broadcast::Sender<MqttMessage>,
    ) -> Result<u32, Mqtt5PubSubError> {
        let mut subscriptions = self.subscriptions.lock().unwrap();
        let mut next_id = self.next_sub_id.lock().unwrap();
        let subscription_id = *next_id;
        *next_id += 1;
        subscriptions.push((topic.clone(), subscription_id, tx.clone()));

        // Replay any retained messages matching this subscription filter
        let retained = self.retained_messages.lock().unwrap();
        for (retained_topic, retained_msg) in retained.iter() {
            if Self::topic_matches(&topic, retained_topic) {
                let mut msg = retained_msg.clone();
                msg.subscription_id = Some(subscription_id);
                let _ = tx.send(msg);
            }
        }

        Ok(subscription_id)
    }

    async fn unsubscribe(&mut self, topic: String) -> Result<(), Mqtt5PubSubError> {
        let mut subscriptions = self.subscriptions.lock().unwrap();
        subscriptions.retain(|(t, _, _)| t != &topic);
        Ok(())
    }

    async fn publish(&mut self, message: MqttMessage) -> Result<MqttPublishSuccess, Mqtt5PubSubError> {
        self.published_messages.lock().unwrap().push(message.clone());
        
        // Return appropriate success variant based on QoS
        match message.qos {
            QoS::AtMostOnce => Ok(MqttPublishSuccess::Sent),
            QoS::AtLeastOnce => Ok(MqttPublishSuccess::Acknowledged),
            QoS::ExactlyOnce => Ok(MqttPublishSuccess::Completed),
        }
    }

    async fn publish_noblock(&mut self, message: MqttMessage) -> tokio::sync::oneshot::Receiver<Result<MqttPublishSuccess, Mqtt5PubSubError>> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.published_messages.lock().unwrap().push(message.clone());
        // Simulate immediate success for mock
        let result = match message.qos {
            QoS::AtMostOnce => Ok(MqttPublishSuccess::Sent),
            QoS::AtLeastOnce => Ok(MqttPublishSuccess::Acknowledged),
            QoS::ExactlyOnce => Ok(MqttPublishSuccess::Completed),
        };
        let _ = tx.send(result);
        rx
    }

    fn publish_nowait(&mut self, message: MqttMessage) -> Result<MqttPublishSuccess, Mqtt5PubSubError> {
        self.published_messages.lock().unwrap().push(message);
        Ok(MqttPublishSuccess::Queued)
    }

    fn get_availability_helper(&mut self) -> crate::availability::AvailabilityHelper {
        crate::availability::AvailabilityHelper::client_availability("local".to_string(), self.client_id.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[tokio::test]
    async fn test_mock_client_creation() {
        let client = MockClient::new("test-client");
        assert_eq!(client.get_client_id(), "test-client");
        let state = *client.get_state().borrow();
        assert_eq!(state, MqttConnectionState::Connected);
        
        let client_default = MockClient::new_default();
        assert_eq!(client_default.get_client_id(), "mock-client");
    }

    #[tokio::test]
    async fn test_mock_client_publish() {
        let mut client = MockClient::new_default();
        
        let msg = MqttMessage::simple(
            "test/topic".to_string(),
            QoS::AtLeastOnce,
            false,
            Bytes::from("test payload"),
        );
        
        let result = client.publish(msg.clone()).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), MqttPublishSuccess::Acknowledged);
        
        let last_msg = client.last_published_message();
        assert!(last_msg.is_some());
        let last_msg = last_msg.unwrap();
        assert_eq!(last_msg.topic, "test/topic");
        assert_eq!(last_msg.payload, Bytes::from("test payload"));
    }

    #[tokio::test]
    async fn test_mock_client_publish_qos_variants() {
        let mut client = MockClient::new_default();
        
        // QoS 0
        let msg0 = MqttMessage::simple(
            "test/topic".to_string(),
            QoS::AtMostOnce,
            false,
            Bytes::from("qos0"),
        );
        let result = client.publish(msg0).await;
        assert_eq!(result.unwrap(), MqttPublishSuccess::Sent);
        
        // QoS 1
        let msg1 = MqttMessage::simple(
            "test/topic".to_string(),
            QoS::AtLeastOnce,
            false,
            Bytes::from("qos1"),
        );
        let result = client.publish(msg1).await;
        assert_eq!(result.unwrap(), MqttPublishSuccess::Acknowledged);
        
        // QoS 2
        let msg2 = MqttMessage::simple(
            "test/topic".to_string(),
            QoS::ExactlyOnce,
            false,
            Bytes::from("qos2"),
        );
        let result = client.publish(msg2).await;
        assert_eq!(result.unwrap(), MqttPublishSuccess::Completed);
    }

    #[tokio::test]
    async fn test_mock_client_publish_nowait() {
        let mut client = MockClient::new_default();
        
        let msg = MqttMessage::simple(
            "test/topic".to_string(),
            QoS::AtMostOnce,
            false,
            Bytes::from("test"),
        );
        
        let result = client.publish_nowait(msg);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), MqttPublishSuccess::Queued);
        
        assert!(client.last_published_message().is_some());
    }

    #[tokio::test]
    async fn test_mock_client_multiple_publishes() {
        let mut client = MockClient::new_default();
        
        for i in 0..5 {
            let msg = MqttMessage::simple(
                format!("test/topic/{}", i),
                QoS::AtLeastOnce,
                false,
                Bytes::from(format!("payload {}", i)),
            );
            client.publish(msg).await.unwrap();
        }
        
        let messages = client.published_messages();
        assert_eq!(messages.len(), 5);
        
        let last = client.last_published_message().unwrap();
        assert_eq!(last.topic, "test/topic/4");
        
        client.clear_published_messages();
        assert_eq!(client.published_messages().len(), 0);
        assert!(client.last_published_message().is_none());
    }

    #[tokio::test]
    async fn test_mock_client_subscribe_and_simulate_receive() {
        let mut client = MockClient::new_default();
        let (tx, mut rx) = broadcast::channel(10);
        
        let sub_id = client.subscribe("test/topic".to_string(), QoS::AtLeastOnce, tx).await;
        assert!(sub_id.is_ok());
        assert_eq!(sub_id.unwrap(), 1);
        
        // Simulate receiving a message
        let msg = MqttMessage::simple(
            "test/topic".to_string(),
            QoS::AtMostOnce,
            false,
            Bytes::from("incoming message"),
        );
        
        let sent_count = client.simulate_receive(msg.clone());
        assert!(sent_count.is_ok());
        assert_eq!(sent_count.unwrap(), 1);
        
        // Verify the message was received with subscription ID
        let received = rx.recv().await;
        assert!(received.is_ok());
        let received_msg = received.unwrap();
        assert_eq!(received_msg.topic, "test/topic");
        assert_eq!(received_msg.payload, Bytes::from("incoming message"));
        assert_eq!(received_msg.subscription_id, Some(1));
    }

    #[tokio::test]
    async fn test_mock_client_unsubscribe() {
        let mut client = MockClient::new_default();
        let (tx, _rx) = broadcast::channel(10);
        
        client.subscribe("test/topic".to_string(), QoS::AtLeastOnce, tx).await.unwrap();
        
        // Unsubscribe
        let result = client.unsubscribe("test/topic".to_string()).await;
        assert!(result.is_ok());
        
        // Simulate receiving a message - should not be delivered
        let msg = MqttMessage::simple(
            "test/topic".to_string(),
            QoS::AtMostOnce,
            false,
            Bytes::from("incoming message"),
        );
        
        let sent_count = client.simulate_receive(msg);
        assert!(sent_count.is_ok());
        assert_eq!(sent_count.unwrap(), 0); // No subscriptions matched
    }

    #[tokio::test]
    async fn test_mock_client_multiple_subscriptions() {
        let mut client = MockClient::new_default();
        let (tx1, mut rx1) = broadcast::channel(10);
        let (tx2, mut rx2) = broadcast::channel(10);
        
        let sub_id1 = client.subscribe("topic1".to_string(), QoS::AtLeastOnce, tx1).await.unwrap();
        let sub_id2 = client.subscribe("topic2".to_string(), QoS::AtLeastOnce, tx2).await.unwrap();
        
        assert_eq!(sub_id1, 1);
        assert_eq!(sub_id2, 2);
        
        // Simulate messages on both topics
        let msg1 = MqttMessage::simple("topic1".to_string(), QoS::AtMostOnce, false, Bytes::from("msg1"));
        let msg2 = MqttMessage::simple("topic2".to_string(), QoS::AtMostOnce, false, Bytes::from("msg2"));
        
        client.simulate_receive(msg1).unwrap();
        client.simulate_receive(msg2).unwrap();
        
        let received1 = rx1.recv().await.unwrap();
        let received2 = rx2.recv().await.unwrap();
        
        assert_eq!(received1.subscription_id, Some(1));
        assert_eq!(received2.subscription_id, Some(2));
    }

    #[tokio::test]
    async fn test_mock_client_retained_message_stored() {
        let client = MockClient::new_default();
        
        let msg = MqttMessage::simple(
            "test/retained".to_string(),
            QoS::AtLeastOnce,
            true,
            Bytes::from("retained payload"),
        );
        
        client.simulate_receive(msg).unwrap();
        
        let retained = client.retained_messages();
        assert_eq!(retained.len(), 1);
        assert!(retained.contains_key("test/retained"));
        assert_eq!(retained["test/retained"].payload, Bytes::from("retained payload"));
    }

    #[tokio::test]
    async fn test_mock_client_retained_message_replaced() {
        let client = MockClient::new_default();
        
        let msg1 = MqttMessage::simple(
            "test/retained".to_string(),
            QoS::AtLeastOnce,
            true,
            Bytes::from("first"),
        );
        let msg2 = MqttMessage::simple(
            "test/retained".to_string(),
            QoS::AtLeastOnce,
            true,
            Bytes::from("second"),
        );
        
        client.simulate_receive(msg1).unwrap();
        client.simulate_receive(msg2).unwrap();
        
        let retained = client.retained_messages();
        assert_eq!(retained.len(), 1);
        assert_eq!(retained["test/retained"].payload, Bytes::from("second"));
    }

    #[tokio::test]
    async fn test_mock_client_non_retained_not_stored() {
        let client = MockClient::new_default();
        
        let msg = MqttMessage::simple(
            "test/topic".to_string(),
            QoS::AtLeastOnce,
            false,
            Bytes::from("not retained"),
        );
        
        client.simulate_receive(msg).unwrap();
        
        assert!(client.retained_messages().is_empty());
    }

    #[tokio::test]
    async fn test_mock_client_subscribe_replays_retained() {
        let mut client = MockClient::new_default();
        
        // Simulate a retained message before subscribing
        let msg = MqttMessage::simple(
            "test/retained".to_string(),
            QoS::AtLeastOnce,
            true,
            Bytes::from("retained payload"),
        );
        client.simulate_receive(msg).unwrap();
        
        // Now subscribe - should immediately receive the retained message
        let (tx, mut rx) = broadcast::channel(10);
        let sub_id = client.subscribe("test/+".to_string(), QoS::AtLeastOnce, tx).await.unwrap();
        
        let received = tokio::time::timeout(std::time::Duration::from_secs(2), rx.recv())
            .await
            .expect("timeout waiting for retained message")
            .unwrap();
        assert_eq!(received.topic, "test/retained");
        assert_eq!(received.payload, Bytes::from("retained payload"));
        assert_eq!(received.subscription_id, Some(sub_id));
    }

    #[tokio::test]
    async fn test_mock_client_subscribe_no_retained_for_topic() {
        let mut client = MockClient::new_default();
        
        // Retain a message on a different topic
        let msg = MqttMessage::simple(
            "other/topic".to_string(),
            QoS::AtLeastOnce,
            true,
            Bytes::from("retained"),
        );
        client.simulate_receive(msg).unwrap();
        
        // Subscribe to a topic with no retained message
        let (tx, mut rx) = broadcast::channel(10);
        client.subscribe("test/topic".to_string(), QoS::AtLeastOnce, tx).await.unwrap();
        
        // Should not receive anything - verify with try_recv
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn test_topic_matches_exact() {
        assert!(MockClient::topic_matches("a/b/c", "a/b/c"));
        assert!(!MockClient::topic_matches("a/b/c", "a/b/d"));
        assert!(!MockClient::topic_matches("a/b", "a/b/c"));
        assert!(!MockClient::topic_matches("a/b/c", "a/b"));
    }

    #[test]
    fn test_topic_matches_single_level_wildcard() {
        assert!(MockClient::topic_matches("a/+/c", "a/b/c"));
        assert!(MockClient::topic_matches("a/+/c", "a/x/c"));
        assert!(!MockClient::topic_matches("a/+/c", "a/b/d"));
        assert!(!MockClient::topic_matches("a/+/c", "a/b/c/d"));
        assert!(MockClient::topic_matches("+", "anything"));
        assert!(!MockClient::topic_matches("+", "two/levels"));
    }

    #[test]
    fn test_topic_matches_multi_level_wildcard() {
        assert!(MockClient::topic_matches("#", "anything"));
        assert!(MockClient::topic_matches("#", "a/b/c"));
        assert!(MockClient::topic_matches("a/#", "a/b"));
        assert!(MockClient::topic_matches("a/#", "a/b/c/d"));
        assert!(MockClient::topic_matches("a/#", "a"));
        assert!(!MockClient::topic_matches("a/#", "b/c"));
    }

    #[test]
    fn test_topic_matches_combined_wildcards() {
        assert!(MockClient::topic_matches("sensor/+/temp/#", "sensor/room1/temp/celsius"));
        assert!(MockClient::topic_matches("sensor/+/temp/#", "sensor/room1/temp"));
        assert!(!MockClient::topic_matches("sensor/+/temp/#", "sensor/room1/humidity"));
    }

    #[tokio::test]
    async fn test_mock_client_subscribe_wildcard_simulate_receive() {
        let mut client = MockClient::new_default();
        let (tx, mut rx) = broadcast::channel(10);

        client.subscribe("test/+".to_string(), QoS::AtLeastOnce, tx).await.unwrap();

        let msg = MqttMessage::simple(
            "test/topic".to_string(),
            QoS::AtMostOnce,
            false,
            Bytes::from("wildcard message"),
        );
        let count = client.simulate_receive(msg).unwrap();
        assert_eq!(count, 1);

        let received = rx.recv().await.unwrap();
        assert_eq!(received.topic, "test/topic");
    }

    #[tokio::test]
    async fn test_mock_client_subscribe_wildcard_replays_retained() {
        let mut client = MockClient::new_default();

        let msg = MqttMessage::simple(
            "test/retained".to_string(),
            QoS::AtLeastOnce,
            true,
            Bytes::from("retained payload"),
        );
        client.simulate_receive(msg).unwrap();

        let (tx, mut rx) = broadcast::channel(10);
        let sub_id = client.subscribe("test/+".to_string(), QoS::AtLeastOnce, tx).await.unwrap();

        let received = rx.try_recv().unwrap();
        assert_eq!(received.topic, "test/retained");
        assert_eq!(received.payload, Bytes::from("retained payload"));
        assert_eq!(received.subscription_id, Some(sub_id));
    }

    #[tokio::test]
    async fn test_mock_client_clear_retained_messages() {
        let mut client = MockClient::new_default();
        
        let msg = MqttMessage::simple(
            "test/retained".to_string(),
            QoS::AtLeastOnce,
            true,
            Bytes::from("retained"),
        );
        client.simulate_receive(msg).unwrap();
        assert_eq!(client.retained_messages().len(), 1);
        
        client.clear_retained_messages();
        assert!(client.retained_messages().is_empty());
    }
}
