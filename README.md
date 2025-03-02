# UPYMQTT

An async implementation for an MQTT client compatible with Micropython.
Includes log handler that publishes log messages to an MQTT topic.

## Example 

```bash
python -m upymqtt.example_telemetry --server rpi1 --port 31883 --user $MQTT_USER --password $MQTT_PASSWORD
```
