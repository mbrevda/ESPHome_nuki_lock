#pragma once
/**
 * @file nuki_bridge.h
 *
 * Nuki Bridge emulation over the Nuki cloud TLS WebSocket protocol.
 * Integrates into NukiLockComponent to proxy cloud commands and lock events.
 */

#include "esphome/core/component.h"
#include "esphome/core/preferences.h"
#include "esphome/components/text_sensor/text_sensor.h"
#include "esphome/components/button/button.h"
#include "esphome/components/event/event.h"

#include <esp_websocket_client.h>
#include <cstdint>
#include <string>
#include <deque>
#include <functional>
#include <mutex>

namespace esphome::nuki_lock {

class NukiLockComponent;

// Cloud connection state
enum class BridgeCloudState : uint8_t {
  Disconnected,
  Connecting,
  Connected,    // WebSocket connected, awaiting BridgeIdResponse
  Identified,   // BridgeId confirmed
  Ready,        // PushNukis acknowledged — fully operational
  Error,
};

// Queued command from the cloud to relay to the lock
struct BridgeRelayRequest {
  std::string payload;          // base64, as sent by the cloud
  uint32_t app_id{0};
  uint32_t nuki_id{0};
  uint32_t device_type{0};
  uint32_t ttl{0};              // seconds; 0 = no expiry
  uint32_t received_ms{0};
  std::string correlation_id;
  std::string characteristic;   // "usdio" or "gdio"
};

class NukiBridge {
 public:
  static constexpr const char *TAG = "nuki_bridge";

  explicit NukiBridge(NukiLockComponent *parent, const std::string &preference_prefix = "nuki_br");

  void setup();
  void loop();
  void dump_config();

  // Configuration setters (from YAML or defaults)
  void set_ws_token(const std::string &token) { this->ws_token_ = token; }
  void set_host_index(uint8_t idx) { this->host_index_ = idx; }
  void set_bridge_id(uint32_t id) { this->bridge_id_ = id; }
  uint32_t get_bridge_id() const { return this->bridge_id_; }
  void set_nuki_id(uint32_t id) { this->nuki_id_ = id; }
  void set_device_type(uint8_t dt) { this->device_type_ = dt; }
  void set_cloud_status_sensor(text_sensor::TextSensor *s) { this->cloud_status_sensor_ = s; }
  void set_cloud_event(event::Event *e) { this->cloud_event_ = e; }

  // Manual or button-triggered registration
  void start_registration();

  // Called when NukiBle sees a lock state-changed advertisement
  void send_keyturner_event();
  // Called after the lock answered a status request; sends a keyturner event if one is owed
  void on_lock_reachable();

  bool is_connected() const { return this->cloud_state_ == BridgeCloudState::Connected || this->cloud_state_ == BridgeCloudState::Identified || this->cloud_state_ == BridgeCloudState::Ready; }
  bool is_ready() const { return this->cloud_state_ == BridgeCloudState::Ready; }
  bool relay_pending() const { return this->relay_in_flight_ || !this->relay_queue_.empty(); }
  BridgeCloudState get_cloud_state() const { return this->cloud_state_; }
  void send_push_nukis_request();
  void send_get_time_request();

 protected:
  NukiLockComponent *parent_{nullptr};
  std::string preference_prefix_;

  std::string ws_token_;
  uint8_t host_index_{0};
  uint32_t bridge_id_{0};
  uint32_t nuki_id_{0};
  uint32_t last_keyturner_event_ms_{0};
  // The cloud marks the lock offline after relay errors and only brings it back on a
  // keyturner event, so one is owed after a failure or an event dropped while not ready.
  bool keyturner_event_owed_{false};
  static const uint32_t FIRMWARE_CHECK_INTERVAL_MS = 24UL * 60 * 60 * 1000;
  uint32_t last_firmware_check_ms_{0};
  uint8_t device_type_{4};

  text_sensor::TextSensor *cloud_status_sensor_{nullptr};
  event::Event *cloud_event_{nullptr};

  // ── WebSocket Client ──
  BridgeCloudState cloud_state_{BridgeCloudState::Disconnected};
  esp_websocket_client_handle_t ws_client_{nullptr};
  uint32_t last_reconnect_ms_{0};
  uint32_t reconnect_interval_ms_{5000};
  uint32_t last_pong_ms_{0};
  uint32_t message_id_{0};

  void cloud_connect();
  void cloud_disconnect();
  void cloud_send(const std::string &json);
  void set_cloud_state(BridgeCloudState state);
  std::string cloud_state_name(BridgeCloudState state);

  // Message handlers
  void on_ws_connected();
  void on_ws_disconnected();
  void on_ws_error();
  void on_ws_data(const char *data, int len);

  void send_bridge_id_request();
  void handle_bridge_id_response(const std::string &json);
  void handle_push_nukis_response(const std::string &json);
  void handle_keyturner_request(const std::string &json);
  void handle_get_time_request(const std::string &json);
  void handle_firmware_message(const std::string &json);
  void check_lock_firmware();

  static void ws_event_handler(void *arg, esp_event_base_t base, int32_t event_id, void *event_data);

  // WebSocket events arrive on the websocket client's own task. Logging or publishing
  // entity state from there corrupts the API stream, so the handler only queues the
  // event and loop() handles it on the main task.
  struct WsEvent {
    int32_t id{0};
    int op_code{0};
    std::string data;
    int error_type{0};
    int handshake_status{0};
    unsigned tls_err{0};
    int sock_errno{0};
  };
  std::mutex ws_events_mutex_;
  std::deque<WsEvent> ws_events_;
  std::string ws_rx_buffer_;  // reassembles fragmented frames (websocket task only)
  void handle_ws_event_(const WsEvent &ev);

  // ── Relay Queue ──
  std::deque<BridgeRelayRequest> relay_queue_;
  bool relay_in_flight_{false};
  BridgeRelayRequest current_relay_;
  uint32_t relay_deadline_ms_{0};

  void relay_prune_queue();
  void relay_process_queue();
  void relay_send_response(const std::string &payload_hex, bool success);
  void relay_send_error(const std::string &error_code);

  // ── Registration & NVS ──
  void generate_autonomous_token();
  void load_credentials();
  void save_credentials();
  bool has_credentials() const { return !this->ws_token_.empty() && this->host_index_ > 0; }

  void fire_event(const std::string &type);
  uint32_t next_message_id() { return ++this->message_id_; }

  ESPPreferenceObject token_pref_;
  ESPPreferenceObject host_pref_;
  ESPPreferenceObject bridge_id_pref_;
};

}  // namespace esphome::nuki_lock
