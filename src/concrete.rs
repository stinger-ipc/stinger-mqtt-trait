use crate::MqttMessage;
use crate::availability_trait::AvailabilityHelper;
use bytes::Bytes;
use jsonpath_rust::JsonPath;
use std::str::FromStr;
use std::time::Duration;

/// A generic implementation of [`AvailabilityHelper`] that publishes JSON online/offline
/// availability messages to `system/{client_id}`.
///
/// # Example
///
/// ```
/// use stinger_mqtt_trait::concrete::GenericAvailability;
/// use stinger_mqtt_trait::availability_trait::AvailabilityHelper;
///
/// let helper = GenericAvailability::new("my-device");
/// assert_eq!(helper.get_client_online_message().topic, "system/my-device");
/// assert_eq!(helper.get_republish_interval(), None);
/// ```
pub struct GenericAvailability {
    online_message: MqttMessage,
    offline_message: MqttMessage,
    republish_interval: Option<Duration>,
}

impl GenericAvailability {
    /// Create a new `GenericAvailability` for the given client ID.
    ///
    /// The topic will be `system/{client_id}`. Online and offline payloads are
    /// `{"online":true}` and `{"online":false}` respectively
    /// , published at QoS 1
    /// with retain enabled and no periodic republish interval.
    pub fn new(client_id: impl Into<String>) -> Self {
        let client_id = client_id.into();
        let topic = format!("system/{}", client_id);
        let online_message = MqttMessage::simple(
            topic.clone(),
            crate::QoS::AtLeastOnce,
            true,
            Bytes::from_static(b"{\"online\":true}"),
        );
        let offline_message = MqttMessage::simple(
            topic,
            crate::QoS::AtLeastOnce,
            true,
            Bytes::from_static(b"{\"online\":false}"),
        );
        Self {
            online_message,
            offline_message,
            republish_interval: None,
        }
    }
}

impl AvailabilityHelper for GenericAvailability {
    fn get_client_online_message(&self) -> MqttMessage {
        self.online_message.clone()
    }

    fn get_client_offline_message(&self) -> MqttMessage {
        self.offline_message.clone()
    }

    fn get_online_json_path(&self) -> JsonPath {
        JsonPath::from_str("$.online").expect("hardcoded path is valid")
    }

    fn get_republish_interval(&self) -> Option<Duration> {
        self.republish_interval
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_online_message() {
        let helper = GenericAvailability::new("device-1");
        let msg = helper.get_client_online_message();
        assert_eq!(msg.topic, "system/device-1");
        assert_eq!(msg.payload, Bytes::from_static(b"{\"online\":true}"));
        assert_eq!(msg.qos, crate::QoS::AtLeastOnce);
        assert!(msg.retain);
    }

    #[test]
    fn test_offline_message() {
        let helper = GenericAvailability::new("device-1");
        let msg = helper.get_client_offline_message();
        assert_eq!(msg.topic, "system/device-1");
        assert_eq!(msg.payload, Bytes::from_static(b"{\"online\":false}"));
        assert_eq!(msg.qos, crate::QoS::AtLeastOnce);
        assert!(msg.retain);
    }

    #[test]
    fn test_republish_interval_none() {
        let helper = GenericAvailability::new("device-1");
        assert_eq!(helper.get_republish_interval(), None);
    }

    #[test]
    fn test_topic_uses_client_id() {
        let helper = GenericAvailability::new("my-system/sensor-a");
        assert_eq!(helper.get_client_online_message().topic, "system/my-system/sensor-a");
        assert_eq!(helper.get_client_offline_message().topic, "system/my-system/sensor-a");
    }

    #[test]
    fn test_online_json_path_returns_expected_path() {
        let helper = GenericAvailability::new("device-1");
        let expected = JsonPath::from_str("$.online").unwrap();
        assert_eq!(helper.get_online_json_path(), expected);
    }

    #[test]
    fn test_online_json_path_matches_online_payload() {
        use jsonpath_rust::JsonPathValue;

        let helper = GenericAvailability::new("device-1");
        let path = helper.get_online_json_path();
        let payload: serde_json::Value =
            serde_json::from_slice(&helper.get_client_online_message().payload)
                .expect("valid JSON payload");
        let results = path.find_slice(&payload);
        assert_eq!(results.len(), 1);
        assert!(matches!(&results[0], JsonPathValue::Slice(v, _) if *v == &serde_json::Value::Bool(true)));
    }

    #[test]
    fn test_online_json_path_matches_offline_payload() {
        use jsonpath_rust::JsonPathValue;

        let helper = GenericAvailability::new("device-1");
        let path = helper.get_online_json_path();
        let payload: serde_json::Value =
            serde_json::from_slice(&helper.get_client_offline_message().payload)
                .expect("valid JSON payload");
        let results = path.find_slice(&payload);
        assert_eq!(results.len(), 1);
        assert!(matches!(&results[0], JsonPathValue::Slice(v, _) if *v == &serde_json::Value::Bool(false)));
    }
}
