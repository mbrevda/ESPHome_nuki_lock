# Unified Nuki Hub: Cloud Bridge & Instant Door Re-Lock

This unified version of the ESPHome Nuki Hub (`feat/nuki-hub-unified`) combines:
1. **Full Nuki Bridge Emulation** over Nuki's cloud TLS WebSocket protocol.
2. **Autonomous Registration** without needing physical bridge credentials or OAuth.
3. **Instant Door Re-locking & State Reporting** (`0x0092 DoorSensorReport`).
4. **Native ESPHome BLE Stack Coexistence** (Bluedroid / `esp32_ble_tracker` / `esp32_ble_client`), allowing other ESPHome BLE sensors/trackers to operate concurrently on the same ESP32.

---

## Features

### 1. Cloud Bridge Emulation
- **Transport**: Persistent TLS WebSocket (`wss://sse{N}-bridge.nuki.io/s/bridge?t={token}&c={bridgeId}`).
- **Autonomous Registration**: Self-generates a standard UUID v4 token and requests a `bridgeId` via `BridgeIdRequest`. Nuki's cloud server accepts autonomous UUID tokens and assigns a valid `bridgeId`.
- **Transparent Encrypted Relay**: The phone app and Nuki Smart Lock communicate with end-to-end encryption. The bridge component acts as a transparent pipe, writing raw frames to the lock's USDIO characteristic and returning the encrypted indications to the cloud.
- **Lock Event Dispatch**: Dispatches `KeyturnerEventRequest` to the cloud immediately when lock status changes or state advertisements are observed.
- **Diagnostic Entities**: Exposes `cloud_status` (Text Sensor) and `cloud_event` (Event Entity) for connection tracking and Home Assistant automations.

### 2. Instant Door Re-locking & Door Sensor Report
- **Sub-Second Door State Reporting**: Relays binary door contact sensor states to the Smart Lock via BLE command `0x0092 DoorSensorReport`.
- **Immediate Auto-Lock on Door Close**: When `auto_lock_on_door_close: true` is configured, closing the door immediately schedules a lock action (`LockAction::Lock`) without waiting for periodic polling intervals.

### 3. Native BLE Coexistence
- Replaces upstream NimBLE with ESPHome's native `esp32_ble_tracker` and `esp32_ble_client` (`BLEClientBase`).
- Freely coexist with Xiaomi BLE sensors, BLE beacons, presence trackers, and other ESPHome BLE components on the same hardware.

---

## Configuration Example

```yaml
esphome:
  name: nuki-unified-hub

esp32:
  board: esp32dev
  framework:
    type: esp-idf

wifi:
  ssid: "MyHomeWiFi"
  password: "SecretPassword"

api:
  custom_services: true
  homeassistant_services: true

logger:
  level: DEBUG

esp32_ble_tracker:

binary_sensor:
  # Example door contact sensor (e.g. GPIO, zigbee via HA, or template)
  - platform: gpio
    pin:
      number: GPIO21
      mode: INPUT_PULLUP
      inverted: true
    id: door_contact_sensor
    name: "Front Door Physical Contact"

lock:
  - platform: nuki_lock
    id: my_nuki_lock
    name: "Front Door Lock"

    # Door sensor integration & instant re-lock
    door_sensor_report:
      door_sensor_input: door_contact_sensor
    auto_lock_on_door_close: true

    # Manual bridge pairing / re-registration button
    pair_bridge:
      name: "Pair Cloud Bridge"

    # Cloud Bridge configuration
    bridge:
      # Optional: set pre-existing credentials if migrating
      # ws_token: "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
      # host_index: 3
      # bridge_id: 123456789
      cloud_status:
        name: "Bridge Cloud Status"
      cloud_event:
        name: "Bridge Cloud Event"
```
