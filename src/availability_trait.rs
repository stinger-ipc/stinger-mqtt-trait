use crate::MqttMessage;
use jsonpath_rust::{JsonPath, JsonPathValue};
use std::time::Duration;

/// Trait for providing MQTT availability (online/offline) messages for a client or service.
///
/// Implementors define the online and offline messages that should be published to the broker,
/// and optionally how often the online message should be republished while the client is active.
pub trait AvailabilityHelper {
    /// Returns the MQTT message to publish when the client comes online.
    fn get_client_online_message(&self) -> MqttMessage;

    /// Returns the topic for the online message.
    fn get_client_online_topic(&self) -> String {
        self.get_client_online_message().topic.clone()
    }

    /// Returns the MQTT message to publish when the client goes offline.
    fn get_client_offline_message(&self) -> MqttMessage;

    /// Returns a [`JsonPath`] that locates the online boolean within the message payload.
    fn get_online_json_path(&self) -> JsonPath;

    /// Returns `true` if the payload of `msg` contains a truthy boolean at the path
    /// returned by [`get_online_json_path`].
    fn client_is_online(&self, msg: MqttMessage) -> bool {
        let path = self.get_online_json_path();
        serde_json::from_slice::<serde_json::Value>(&msg.payload)
            .ok()
            .map(|v| {
                path.find_slice(&v).iter().any(|r| {
                    matches!(r, JsonPathValue::Slice(b, _) if b.as_bool() == Some(true))
                })
            })
            .unwrap_or(false)
    }

    /// Returns how often the online message should be republished, or `None` if no
    /// periodic republishing is desired.
    fn get_republish_interval(&self) -> Option<Duration>;
}
