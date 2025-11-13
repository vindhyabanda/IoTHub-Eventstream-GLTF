# Real-Time Rack Monitoring System
This project is a proof of concept demonstrating how IoT telemetry can influence updates to a 3D visualization environment in real time. This lightweight architecture features the following components:
- IoT temperature simulation to Azure IoT Hub via MQTT
- Real-time status updates via Azure Eventstream (Kafka endpoint)
- 3D visualization of server racks with live updates

## Architecture

```
IoT Simulator → Azure IoT Hub → Azure Eventstream → Kafka Consumer → WebSocket → 3D Web Viewer
```

## Quick Start

### Prerequisites
1. Copy `config.template.py` to `config.py` and update it with your Azure credentials.
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

### Running the System

1. Start the IoT Temperature Simulator:
   ```bash
   python bms_data_mqtt_simulator.py
   ```
2. Start the Kafka Consumer and WebSocket Bridge:
   ```bash
   python kafka_consumer.py
   ```
3. Start the Web Server:
   ```bash
   python serve_usd_viewer.py
   ```
4. Open the 3D Viewer in your browser:
   ```
   http://localhost:8080/simple_usd_viewer.html
   ```

## Features

- Real-time 3D visualization
- Kafka streaming integration
- WebSocket communication
- Status-based color updates
- Interactive 3D controls
- Azure IoT Hub integration

## Troubleshooting

### Common Issues

- **Config module not found**: Ensure `config.py` exists and is correctly configured.
- **WebSocket connection issues**: Verify `kafka_consumer.py` is running and port 8767 is accessible.
- **No color changes in the viewer**: Check that the IoT simulator is sending data and the Kafka consumer is processing messages.