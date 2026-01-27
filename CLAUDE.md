# CLAUDE.md

## Project Overview

mqttheat is a Python-based multi-zone radiant heating/cooling control system that communicates via MQTT. It uses PID control with weather-aware optimization to manage room temperatures and a centralized heat pump.

## Tech Stack

- **Language:** Python 3.11
- **Dependencies:** paho-mqtt, PyYAML, simple-pid, python-json-logger (see `requirements.txt`)
- **Deployment:** Docker (docker-compose)

## Project Structure

```
src/
  mqtt_heat.py      # Main application entry point and control loop
  room_control.py   # Per-room PID/on-off temperature control
  sensor.py         # MQTT sensor data wrapper with timeout tracking
  pid.py            # Custom PID controller with derivative filtering and anti-windup
```

## Build & Run

Run locally:
```bash
cp config.example.yml config.yml   # then edit with your MQTT broker details
python src/mqtt_heat.py config.yml
```

Set `LOGLEVEL` environment variable to control log verbosity (default: INFO).

## Configuration

Copy `config.example.yml` to `config.yml` (gitignored). Key sections:
- MQTT broker connection
- `topic_prefix` for MQTT topic namespace
- `latitude`/`longitude` for weather forecast integration
- `pump_topic` for optional water pump control
- `rooms` map with heat/humidity sensors and output topics (heat level) per room

## Architecture

Event-driven pub/sub via MQTT with a periodic control loop (default 15 min):
1. Receive sensor updates and forecast data via MQTT
2. Calculate weather-based PID modifiers (temperature forecasts, UV, wind)
3. Update each room's heating/cooling output via PID or on/off control
4. Publish state to MQTT (including Home Assistant auto-discovery)
5. Control pump via PWM based on aggregate heating demand

Key algorithms: night heating cosine modifier, keep-warm mode (thermal mass tracking), heat offset compensation (overshoot prevention), pump duty cycle management.

## Code Conventions

- No test suite exists
- Logging uses JSON format via python-json-logger
- Configuration is plain YAML; runtime config updates arrive via MQTT
- Module dependency chain: `mqtt_heat.py` -> `room_control.py` -> `pid.py` / `sensor.py`
