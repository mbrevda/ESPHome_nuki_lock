#include "nuki_bridge.h"
#include "nuki_lock.h"
#include "esphome/core/log.h"
#include "esphome/core/helpers.h"
#include "esphome/core/alloc_helpers.h"
#include "esphome/core/application.h"
#include "esphome/components/network/util.h"

#include <esp_websocket_client.h>
#include <esp_tls.h>
#include <esp_crt_bundle.h>
#include <esp_random.h>
#include <sys/time.h>
#include <cstring>
#include <cinttypes>
#include <cstdio>
#include <algorithm>

namespace esphome::nuki_lock {

static const char *const TAG = "nuki_bridge";

// ═══════════════════════════════════════════════════════════════
//  Minimal JSON helpers
// ═══════════════════════════════════════════════════════════════

static std::string json_string_field(const std::string &json, const char *key) {
  char pat[64];
  snprintf(pat, sizeof(pat), "\"%s\"", key);
  auto pos = json.find(pat);
  if (pos == std::string::npos) return {};
  pos = json.find(':', pos);
  if (pos == std::string::npos) return {};
  pos = json.find('"', pos + 1);
  if (pos == std::string::npos) return {};
  auto end = json.find('"', pos + 1);
  if (end == std::string::npos) return {};
  return json.substr(pos + 1, end - pos - 1);
}

static int64_t json_int_field(const std::string &json, const char *key, int64_t def = -1) {
  char pat[64];
  snprintf(pat, sizeof(pat), "\"%s\"", key);
  auto pos = json.find(pat);
  if (pos == std::string::npos) return def;
  pos = json.find(':', pos);
  if (pos == std::string::npos) return def;
  pos++;
  while (pos < json.size() && json[pos] == ' ') pos++;
  char *end;
  long long val = strtoll(json.c_str() + pos, &end, 10);
  if (end == json.c_str() + pos) return def;
  return val;
}

static std::string json_payload_field(const std::string &json) {
  return json_string_field(json, "payload");
}

// ═══════════════════════════════════════════════════════════════
//  Constructor & Lifecycle
// ═══════════════════════════════════════════════════════════════

NukiBridge::NukiBridge(NukiLockComponent *parent, const std::string &preference_prefix)
    : parent_(parent), preference_prefix_(preference_prefix) {}

void NukiBridge::setup() {
  bool yaml_token_provided = !this->ws_token_.empty();
  this->load_credentials();

  if (yaml_token_provided) {
    ESP_LOGI(TAG, "Using credentials from YAML configuration");
    this->save_credentials();
  } else if (this->has_credentials()) {
    ESP_LOGI(TAG, "Cloud credentials present (host_index=%u, bridge_id=%" PRIu32 ")",
             this->host_index_, this->bridge_id_);
  } else {
    ESP_LOGI(TAG, "No credentials — generating autonomous bridge token");
    this->generate_autonomous_token();
  }
  this->set_cloud_state(BridgeCloudState::Disconnected);
}

void NukiBridge::loop() {
  // Handle WebSocket events queued by the websocket task.
  while (true) {
    WsEvent ev;
    {
      std::lock_guard<std::mutex> lock(this->ws_events_mutex_);
      if (this->ws_events_.empty())
        break;
      ev = std::move(this->ws_events_.front());
      this->ws_events_.pop_front();
    }
    this->handle_ws_event_(ev);
  }

  // Cloud connection reconnection logic
  if (this->has_credentials() && this->cloud_state_ == BridgeCloudState::Disconnected) {
    uint32_t now = millis();
    if (now - this->last_reconnect_ms_ >= this->reconnect_interval_ms_) {
      this->last_reconnect_ms_ = now;
      this->cloud_connect();
    }
  }

  this->check_lock_firmware();

  // Relay queue pruning and processing
  this->relay_prune_queue();
  this->relay_process_queue();
}

void NukiBridge::dump_config() {
  ESP_LOGCONFIG(TAG, "Nuki Bridge Emulator:");
  ESP_LOGCONFIG(TAG, "  Host: sse%u-bridge.nuki.io", this->host_index_);
  ESP_LOGCONFIG(TAG, "  Bridge ID: %" PRIu32, this->bridge_id_);
  ESP_LOGCONFIG(TAG, "  Token: %s", this->ws_token_.empty() ? "(none)" : "(set)");
  ESP_LOGCONFIG(TAG, "  Cloud state: %s", this->cloud_state_name(this->cloud_state_).c_str());
}

// ═══════════════════════════════════════════════════════════════
//  Cloud WebSocket
// ═══════════════════════════════════════════════════════════════

void NukiBridge::cloud_connect() {
  if (this->ws_client_) {
    esp_websocket_client_destroy(this->ws_client_);
    this->ws_client_ = nullptr;
  }

  // Matches the Bridge v2 Wi-Fi firmware: "?t=<token>" until a bridgeId is assigned,
  // then "?c=<bridgeId>&t=<token>".
  char uri[160];
  if (this->bridge_id_ != 0) {
    snprintf(uri, sizeof(uri), "wss://sse%u-bridge.nuki.io/s/bridge?c=%" PRIu32 "&t=%s",
             this->host_index_, this->bridge_id_, this->ws_token_.c_str());
  } else {
    snprintf(uri, sizeof(uri), "wss://sse%u-bridge.nuki.io/s/bridge?t=%s",
             this->host_index_, this->ws_token_.c_str());
  }

  esp_websocket_client_config_t cfg = {};
  cfg.uri = uri;
  cfg.task_stack = 6144;
  cfg.buffer_size = 2048;
  cfg.cert_pem = nullptr;
  cfg.crt_bundle_attach = esp_crt_bundle_attach;
  cfg.reconnect_timeout_ms = 5000;
  cfg.disable_auto_reconnect = true;
  cfg.network_timeout_ms = 10000;
  cfg.ping_interval_sec = 10;
  cfg.pingpong_timeout_sec = 20;

  this->ws_client_ = esp_websocket_client_init(&cfg);
  if (!this->ws_client_) {
    ESP_LOGE(TAG, "Failed to init WebSocket client");
    this->set_cloud_state(BridgeCloudState::Error);
    return;
  }

  esp_websocket_register_events(this->ws_client_, WEBSOCKET_EVENT_ANY, ws_event_handler, this);
  this->set_cloud_state(BridgeCloudState::Connecting);

  if (esp_websocket_client_start(this->ws_client_) != ESP_OK) {
    ESP_LOGE(TAG, "Failed to start WebSocket client");
    esp_websocket_client_destroy(this->ws_client_);
    this->ws_client_ = nullptr;
    this->set_cloud_state(BridgeCloudState::Error);
  }
}

void NukiBridge::cloud_disconnect() {
  if (this->ws_client_) {
    esp_websocket_client_close(this->ws_client_, pdMS_TO_TICKS(1000));
    esp_websocket_client_destroy(this->ws_client_);
    this->ws_client_ = nullptr;
  }
  this->set_cloud_state(BridgeCloudState::Disconnected);
}

void NukiBridge::cloud_send(const std::string &json) {
  if (!this->ws_client_ || !esp_websocket_client_is_connected(this->ws_client_)) {
    ESP_LOGW(TAG, "Cannot send — WebSocket not connected");
    return;
  }
  ESP_LOGD(TAG, ">>> %s", json.c_str());
  esp_websocket_client_send_text(this->ws_client_, json.c_str(), json.size(), pdMS_TO_TICKS(1000));
}

void NukiBridge::ws_event_handler(void *arg, esp_event_base_t base, int32_t event_id, void *event_data) {
  // Runs on the websocket task: no logging or entity updates here (see WsEvent).
  auto *self = static_cast<NukiBridge *>(arg);
  auto *data = static_cast<esp_websocket_event_data_t *>(event_data);

  WsEvent ev;
  ev.id = event_id;
  switch (event_id) {
    case WEBSOCKET_EVENT_CONNECTED:
    case WEBSOCKET_EVENT_DISCONNECTED:
      break;
    case WEBSOCKET_EVENT_DATA:
      if (data->op_code == 9 || data->op_code == 10)
        return;
      if (data->op_code == 1 || data->op_code == 2 || data->op_code == 0) {
        // Text/binary frame, possibly delivered in pieces.
        if (data->payload_offset == 0)
          self->ws_rx_buffer_.clear();
        if (data->data_len > 0)
          self->ws_rx_buffer_.append(data->data_ptr, data->data_len);
        if (data->payload_offset + data->data_len < data->payload_len)
          return;
        ev.op_code = data->op_code == 0 ? 1 : data->op_code;
        ev.data.swap(self->ws_rx_buffer_);
      } else {
        ev.op_code = data->op_code;
        if (data->data_len > 0)
          ev.data.assign(data->data_ptr, data->data_len);
      }
      break;
    case WEBSOCKET_EVENT_ERROR:
      ev.error_type = (int) data->error_handle.error_type;
      ev.handshake_status = data->error_handle.esp_ws_handshake_status_code;
      ev.tls_err = (unsigned) data->error_handle.esp_tls_last_esp_err;
      ev.sock_errno = data->error_handle.esp_transport_sock_errno;
      break;
    default:
      return;
  }
  std::lock_guard<std::mutex> lock(self->ws_events_mutex_);
  self->ws_events_.push_back(std::move(ev));
}

void NukiBridge::handle_ws_event_(const WsEvent &ev) {
  switch (ev.id) {
    case WEBSOCKET_EVENT_CONNECTED:
      this->on_ws_connected();
      break;
    case WEBSOCKET_EVENT_DISCONNECTED:
      this->on_ws_disconnected();
      break;
    case WEBSOCKET_EVENT_DATA:
      if (ev.op_code == 1 && !ev.data.empty()) {
        this->on_ws_data(ev.data.data(), ev.data.size());
      } else if (ev.op_code == 2 && !ev.data.empty()) {
        ESP_LOGW(TAG, "Binary frame (%u bytes): %s", (unsigned) ev.data.size(),
                 format_hex_pretty((const uint8_t *) ev.data.data(), std::min<size_t>(ev.data.size(), 64)).c_str());
        this->on_ws_data(ev.data.data(), ev.data.size());
      } else if (ev.op_code == 8) {
        int code = ev.data.size() >= 2 ? (((uint8_t) ev.data[0]) << 8) | (uint8_t) ev.data[1] : -1;
        std::string reason = ev.data.size() > 2 ? ev.data.substr(2) : "";
        ESP_LOGW(TAG, "Server closed the socket: code=%d reason='%s'", code, reason.c_str());
      } else {
        ESP_LOGD(TAG, "WS frame op=%d len=%u", ev.op_code, (unsigned) ev.data.size());
      }
      break;
    case WEBSOCKET_EVENT_ERROR:
      ESP_LOGW(TAG, "WS error: type=%d handshake_status=%d tls=0x%x sock_errno=%d", ev.error_type,
               ev.handshake_status, ev.tls_err, ev.sock_errno);
      this->on_ws_error();
      break;
    default:
      break;
  }
}

void NukiBridge::on_ws_connected() {
  ESP_LOGI(TAG, "Cloud WebSocket connected");
  this->set_cloud_state(BridgeCloudState::Connected);
  this->last_pong_ms_ = millis();
  this->fire_event("connected");
  // The server mints a fresh bridgeId for every BridgeIdRequest, so only ask when we
  // have none; an existing id (from YAML or NVS) must be kept, not replaced.
  if (this->bridge_id_ == 0) {
    this->send_bridge_id_request();
  } else {
    this->set_cloud_state(BridgeCloudState::Identified);
    this->send_push_nukis_request();
    this->send_get_time_request();
  }
}

void NukiBridge::on_ws_disconnected() {
  ESP_LOGW(TAG, "Cloud WebSocket disconnected");
  this->set_cloud_state(BridgeCloudState::Disconnected);
  this->fire_event("disconnected");
  if (this->reconnect_interval_ms_ < 60000) {
    this->reconnect_interval_ms_ *= 2;
  }
}

void NukiBridge::on_ws_error() {
  ESP_LOGE(TAG, "Cloud WebSocket error");
  this->set_cloud_state(BridgeCloudState::Error);
  this->fire_event("error");
}

void NukiBridge::on_ws_data(const char *data, int len) {
  std::string json(data, len);
  ESP_LOGD(TAG, "<<< %s", json.c_str());

  auto name = json_string_field(json, "name");
  if (name.empty()) {
    ESP_LOGW(TAG, "Cloud message missing 'name'");
    return;
  }

  if (name == "BridgeIdResponse") {
    this->handle_bridge_id_response(json);
  } else if (name == "PushNukisResponse") {
    this->handle_push_nukis_response(json);
  } else if (name == "KeyturnerRequest") {
    this->handle_keyturner_request(json);
  } else if (name == "KeyturnerEventResponse") {
    ESP_LOGD(TAG, "KeyturnerEventResponse received");
  } else if (name == "GetTimeResponse") {
    ESP_LOGD(TAG, "GetTimeResponse received");
  } else if (name == "GetTimeRequest") {
    this->handle_get_time_request(json);
  } else if (name.find("Firmware") != std::string::npos) {
    this->handle_firmware_message(json);
  } else {
    ESP_LOGD(TAG, "Unhandled cloud message: %s", name.c_str());
  }
}

void NukiBridge::send_bridge_id_request() {
  this->cloud_send("{\"name\":\"BridgeIdRequest\"}");
}

void NukiBridge::send_push_nukis_request() {
  uint32_t lock_id = (this->parent_ && this->parent_->get_lock_nuki_id() != 0)
                         ? this->parent_->get_lock_nuki_id()
                         : this->nuki_id_;
  uint32_t dev_type = this->device_type_;
  if (this->parent_ && this->parent_->get_nuki_lock_config() &&
      this->parent_->get_nuki_lock_config()->deviceType != 255) {
    dev_type = this->parent_->get_nuki_lock_config()->deviceType;
  }

  // Same shape as a real Bridge v2: {"deviceType","nukiId","uuid","rssi"} per lock,
  // then the bridge's LAN address (0.0.0.0 until it has one).
  char ip[network::IP_ADDRESS_BUFFER_SIZE] = "0.0.0.0";
  for (auto &addr : network::get_ip_addresses()) {
    if (addr.is_ip4() && addr.is_set()) {
      addr.str_to(ip);
      break;
    }
  }

  char nuki[160] = "";
  if (lock_id != 0) {
    int rssi = 0;
    char uuid[37];
    bool has_uuid = false;
    if (this->parent_) {
      rssi = this->parent_->get_nuki_lock()->get_rssi();
      has_uuid = this->parent_->get_nuki_lock()->get_lock_uuid(uuid, sizeof(uuid));
    }
    // The cloud drops the socket when "uuid" is missing. The lock only sends its UUID
    // while pairing, so a pairing made before it was stored gets a placeholder.
    if (!has_uuid) {
      strcpy(uuid, "00000000-0000-0000-0000-000000000000");
    }
    snprintf(nuki, sizeof(nuki),
             "{\"deviceType\":%" PRIu32 ",\"nukiId\":%" PRIu32 ",\"uuid\":\"%s\",\"rssi\":%d}",
             dev_type, lock_id, uuid, rssi);
  }

  char buf[320];
  snprintf(buf, sizeof(buf),
           "{\"name\":\"PushNukisRequest\",\"nukis\":[%s],\"ip\":\"%s\",\"port\":8080}",
           nuki, ip);
  this->cloud_send(buf);
}

void NukiBridge::send_get_time_request() {
  this->cloud_send("{\"name\":\"GetTimeRequest\"}");
}

void NukiBridge::handle_bridge_id_response(const std::string &json) {
  auto err = json_int_field(json, "errorCode", -1);
  if (err != 0) {
    ESP_LOGE(TAG, "BridgeIdResponse errorCode=%" PRId64, err);
    this->set_cloud_state(BridgeCloudState::Error);
    return;
  }
  auto bid = json_int_field(json, "bridgeId", 0);
  if (bid > 0 && this->bridge_id_ != 0 && (uint32_t) bid != this->bridge_id_) {
    ESP_LOGW(TAG, "Ignoring new bridgeId from cloud; keeping the configured one");
  } else if (bid > 0) {
    this->bridge_id_ = (uint32_t) bid;
    this->save_credentials();
    ESP_LOGI(TAG, "Bridge ID confirmed & saved to NVS: %" PRIu32, this->bridge_id_);
  }
  this->set_cloud_state(BridgeCloudState::Identified);
  this->fire_event("bridge_id_received");
  this->send_push_nukis_request();
  this->send_get_time_request();
}

void NukiBridge::handle_push_nukis_response(const std::string &json) {
  auto err = json_int_field(json, "errorCode", -1);
  if (err != 0) {
    ESP_LOGE(TAG, "PushNukisResponse errorCode=%" PRId64, err);
    this->set_cloud_state(BridgeCloudState::Error);
    return;
  }
  ESP_LOGI(TAG, "Nuki Bridge cloud session ready");
  this->reconnect_interval_ms_ = 5000;
  this->set_cloud_state(BridgeCloudState::Ready);
  // Tell the cloud the lock is reachable; otherwise it stays offline after an outage
  // until the lock's state happens to change.
  this->send_keyturner_event();
}

void NukiBridge::handle_keyturner_request(const std::string &json) {
  ESP_LOGI(TAG, "KeyturnerRequest received from cloud");
  this->fire_event("keyturner_request");

  // The cloud sends base64; JSON may escape '/' as "\/".
  auto payload_b64 = json_payload_field(json);
  payload_b64.erase(std::remove(payload_b64.begin(), payload_b64.end(), '\\'), payload_b64.end());
  auto app_id = (uint32_t) json_int_field(json, "appId", 0);
  auto correlation = json_string_field(json, "correlationId");
  auto characteristic = json_string_field(json, "characteristic");
  auto nuki_id = (uint32_t) json_int_field(json, "nukiId", 0);
  auto device_type = (uint32_t) json_int_field(json, "deviceType", 4);
  auto ttl = (uint32_t) json_int_field(json, "ttl", 0);

  if (payload_b64.empty()) {
    ESP_LOGW(TAG, "KeyturnerRequest with empty payload");
    this->relay_send_error("PAYLOAD_EMPTY");
    return;
  }

  if (characteristic.empty()) characteristic = "usdio";

  BridgeRelayRequest req;
  req.payload = payload_b64;
  req.app_id = app_id;
  req.correlation_id = correlation;
  req.characteristic = characteristic;
  req.nuki_id = nuki_id;
  req.device_type = device_type;
  req.ttl = ttl;
  req.received_ms = millis();
  this->relay_queue_.push_back(std::move(req));

  ESP_LOGD(TAG, "Queued relay request (queue=%u)", (unsigned) this->relay_queue_.size());
}

void NukiBridge::handle_get_time_request(const std::string &json) {
  auto correlation = json_string_field(json, "correlationId");
  char buf[128];
  struct timeval tv;
  gettimeofday(&tv, nullptr);
  snprintf(buf, sizeof(buf),
           "{\"name\":\"GetTimeResponse\",\"time\":%" PRId64 ",\"correlationId\":\"%s\"}",
           (int64_t) tv.tv_sec, correlation.c_str());
  this->cloud_send(buf);
}

// Firmware updates are not implemented yet (see README). The bridge asks the cloud for
// the lock's latest firmware once a day and only logs the answer; nothing is downloaded.
void NukiBridge::check_lock_firmware() {
  if (this->cloud_state_ != BridgeCloudState::Ready || this->parent_ == nullptr) return;
  uint32_t now = millis();
  if (this->last_firmware_check_ms_ != 0 && now - this->last_firmware_check_ms_ < FIRMWARE_CHECK_INTERVAL_MS) return;
  uint32_t lock_id = this->parent_->get_lock_nuki_id();
  const unsigned char *ver = this->parent_->get_nuki_lock_config()->firmwareVersion;
  if (lock_id == 0 || (ver[0] == 0 && ver[1] == 0 && ver[2] == 0)) return;  // lock config not read yet
  this->last_firmware_check_ms_ = now;

  char buf[192];
  snprintf(buf, sizeof(buf),
           "{\"name\":\"KeyturnerFirmwareUpdateRequest\",\"nukiId\":%" PRIu32
           ",\"deviceType\":%u,\"currentVersion\":[%u,%u,%u],\"type\":0}",
           lock_id, (unsigned) this->device_type_, ver[0], ver[1], ver[2]);
  this->cloud_send(buf);
  ESP_LOGI(TAG, "Asked the cloud for lock firmware updates (current %u.%u.%u)", ver[0], ver[1], ver[2]);
}

void NukiBridge::handle_firmware_message(const std::string &json) {
  // The reply format is unconfirmed, so log it whole; no reply and no download.
  if (json.find("\"updateAvailable\":true") != std::string::npos) {
    ESP_LOGW(TAG, "Firmware update available (not applied): %s", json.c_str());
  } else {
    ESP_LOGI(TAG, "Firmware message (no action taken): %s", json.c_str());
  }
}

void NukiBridge::send_keyturner_event() {
  if (this->cloud_state_ != BridgeCloudState::Ready) {
    this->keyturner_event_owed_ = true;
    return;
  }
  uint32_t lock_id = this->parent_ ? this->parent_->get_lock_nuki_id() : 0;
  if (lock_id == 0) return;
  // The lock signals one change several times in a row; the cloud only needs to hear once.
  uint32_t now = millis();
  if (this->last_keyturner_event_ms_ != 0 && now - this->last_keyturner_event_ms_ < 1000) return;
  this->last_keyturner_event_ms_ = now;
  this->keyturner_event_owed_ = false;

  char buf[128];
  snprintf(buf, sizeof(buf),
           "{\"name\":\"KeyturnerEventRequest\","
           "\"nukiId\":%" PRIu32 ","
           "\"deviceType\":%u}",
           lock_id, (unsigned) this->device_type_);
  this->cloud_send(buf);
  this->fire_event("keyturner_event");
  ESP_LOGI(TAG, "Sent KeyturnerEventRequest for lock %" PRIu32, lock_id);
}

void NukiBridge::on_lock_reachable() {
  if (this->keyturner_event_owed_) this->send_keyturner_event();
}

// ═══════════════════════════════════════════════════════════════
//  Relay Processing
// ═══════════════════════════════════════════════════════════════

void NukiBridge::relay_prune_queue() {
  while (!this->relay_queue_.empty()) {
    auto &front = this->relay_queue_.front();
    if (front.ttl > 0 && (millis() - front.received_ms) / 1000 >= front.ttl) {
      ESP_LOGW(TAG, "Relay request expired (ttl=%" PRIu32 "s)", front.ttl);
      this->relay_queue_.pop_front();
    } else {
      break;
    }
  }
}

void NukiBridge::relay_process_queue() {
  if (this->relay_in_flight_) {
    if (millis() > this->relay_deadline_ms_) {
      ESP_LOGW(TAG, "Relay deadline exceeded");
      this->relay_send_error("TIMEOUT");
      this->relay_in_flight_ = false;
    }
    return;
  }

  if (this->relay_queue_.empty()) return;
  if (!this->parent_ || !this->parent_->can_relay()) return;

  this->current_relay_ = this->relay_queue_.front();
  this->relay_queue_.pop_front();

  uint8_t payload[512];
  size_t len = base64_decode(this->current_relay_.payload, payload, sizeof(payload));
  if (len == 0) {
    this->relay_send_error("INVALID_PAYLOAD");
    return;
  }

  this->relay_in_flight_ = true;
  // Backstop only: NukiBle reports its own 15s timeout first.
  this->relay_deadline_ms_ = millis() + 20000;

  bool ok = this->parent_->execute_relay_command(
      payload, len,
      [this](bool success, const uint8_t *res, size_t res_len) {
        if (success && res != nullptr && res_len > 0) {
          // Each lock indication goes back as its own KeyturnerResponse.
          this->relay_send_response(base64_encode(res, res_len), true);
          this->fire_event("keyturner_response");
          this->relay_in_flight_ = false;
        } else if (!success && this->relay_in_flight_) {
          this->relay_send_error("RELAY_FAILED");
          this->relay_in_flight_ = false;
        }
      });

  if (!ok) {
    this->relay_send_error("LINK_BUSY");
    this->relay_in_flight_ = false;
  }
}

// Response formats are copied from the Bridge v2 Wi-Fi firmware, which echoes appId and
// sends no correlationId.
void NukiBridge::relay_send_response(const std::string &payload_b64, bool success) {
  char buf[1536];
  snprintf(buf, sizeof(buf),
           "{\"name\":\"KeyturnerResponse\",\"deviceType\":%" PRIu32 ",\"nukiId\":%" PRIu32
           ",\"appId\":%" PRIu32 ",\"payload\":\"%s\",\"characteristic\":\"%s\"}",
           this->current_relay_.device_type, this->current_relay_.nuki_id, this->current_relay_.app_id,
           payload_b64.c_str(), this->current_relay_.characteristic.c_str());
  this->cloud_send(buf);
}

void NukiBridge::relay_send_error(const std::string &error_code) {
  // The firmware only has a numeric errorCode; the meaning of its values is unknown, so
  // every failure is reported as 1 and the reason only logged.
  ESP_LOGW(TAG, "Relay failed: %s", error_code.c_str());
  this->keyturner_event_owed_ = true;
  char buf[256];
  snprintf(buf, sizeof(buf),
           "{\"name\":\"KeyturnerResponse\",\"deviceType\":%" PRIu32 ",\"nukiId\":%" PRIu32
           ",\"appId\":%" PRIu32 ",\"errorCode\":\"1\"}",
           this->current_relay_.device_type, this->current_relay_.nuki_id, this->current_relay_.app_id);
  this->cloud_send(buf);
}

// ═══════════════════════════════════════════════════════════════
//  Registration & Persistence
// ═══════════════════════════════════════════════════════════════

void NukiBridge::generate_autonomous_token() {
  uint8_t rand_bytes[16];
  esp_fill_random(rand_bytes, 16);
  rand_bytes[6] = (rand_bytes[6] & 0x0F) | 0x40;  // UUID v4
  rand_bytes[8] = (rand_bytes[8] & 0x3F) | 0x80;  // variant 1
  char uuid_str[37];
  snprintf(uuid_str, sizeof(uuid_str),
           "%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x",
           rand_bytes[0], rand_bytes[1], rand_bytes[2], rand_bytes[3],
           rand_bytes[4], rand_bytes[5], rand_bytes[6], rand_bytes[7],
           rand_bytes[8], rand_bytes[9], rand_bytes[10], rand_bytes[11],
           rand_bytes[12], rand_bytes[13], rand_bytes[14], rand_bytes[15]);
  this->ws_token_ = uuid_str;
  this->bridge_id_ = 0;
  // sse1..sse9-bridge.nuki.io all resolve; spread new bridges across them.
  this->host_index_ = 1 + esp_random() % 9;
  this->save_credentials();
  ESP_LOGI(TAG, "Generated autonomous bridge token: %s (sse%u)", this->ws_token_.c_str(), this->host_index_);
}

void NukiBridge::start_registration() {
  ESP_LOGI(TAG, "Re-registering bridge: minting fresh autonomous token...");
  this->cloud_disconnect();
  this->generate_autonomous_token();
  this->set_cloud_state(BridgeCloudState::Disconnected);
}

void NukiBridge::load_credentials() {
  uint32_t hash_base = fnv1_hash(this->preference_prefix_);

  this->token_pref_ = global_preferences->make_preference<uint8_t[37]>(hash_base + 1);
  this->host_pref_ = global_preferences->make_preference<uint8_t>(hash_base + 2);
  this->bridge_id_pref_ = global_preferences->make_preference<uint32_t>(hash_base + 3);

  // If credentials already provided (e.g. from YAML), do not overwrite with stale NVS data
  if (!this->ws_token_.empty()) {
    return;
  }

  uint8_t saved_token[37]{};
  if (this->token_pref_.load(&saved_token)) {
    std::string t(reinterpret_cast<char *>(saved_token), strnlen(reinterpret_cast<char *>(saved_token), 36));
    if (!t.empty()) {
      this->ws_token_ = t;
      ESP_LOGD(TAG, "Loaded bridge token from NVS");
    }
  }

  uint8_t saved_host{0};
  if (this->host_pref_.load(&saved_host) && saved_host > 0) {
    this->host_index_ = saved_host;
    ESP_LOGD(TAG, "Loaded host_index from NVS: %u", this->host_index_);
  }

  uint32_t saved_bid{0};
  if (this->bridge_id_pref_.load(&saved_bid) && saved_bid > 0) {
    this->bridge_id_ = saved_bid;
    ESP_LOGD(TAG, "Loaded bridge_id from NVS: %" PRIu32, this->bridge_id_);
  }
}

void NukiBridge::save_credentials() {
  uint8_t token_buf[37]{};
  size_t copy_len = std::min(this->ws_token_.size(), (size_t) 36);
  memcpy(token_buf, this->ws_token_.c_str(), copy_len);
  this->token_pref_.save(&token_buf);
  this->host_pref_.save(&this->host_index_);
  this->bridge_id_pref_.save(&this->bridge_id_);
  ESP_LOGI(TAG, "Bridge credentials saved to NVS");
}

void NukiBridge::set_cloud_state(BridgeCloudState state) {
  this->cloud_state_ = state;
  if (this->cloud_status_sensor_) {
    this->cloud_status_sensor_->publish_state(this->cloud_state_name(state));
  }
}

std::string NukiBridge::cloud_state_name(BridgeCloudState state) {
  switch (state) {
    case BridgeCloudState::Disconnected: return "disconnected";
    case BridgeCloudState::Connecting:   return "connecting";
    case BridgeCloudState::Connected:    return "connected";
    case BridgeCloudState::Identified:   return "identified";
    case BridgeCloudState::Ready:        return "registered";
    case BridgeCloudState::Error:        return "error";
    default:                             return "unknown";
  }
}

void NukiBridge::fire_event(const std::string &type) {
  if (this->cloud_event_) {
    this->cloud_event_->trigger(type);
  }
}

}  // namespace esphome::nuki_lock
