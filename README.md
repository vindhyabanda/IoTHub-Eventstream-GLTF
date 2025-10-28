# Real-Time Rack Monitoring System

A comprehensive IoT monitoring solution that combines:
- IoT temperature simulation to Azure IoT Hub via MQTT
- Real-time status updates via Azure Event Hub (Kafka)
- 3D visualization of server racks with live color updates

## 🎯 Architecture

```
IoT Simulator → Azure IoT Hub → Azure Event Hub → Kafka Consumer → WebSocket → 3D Web Viewer
     ↓              ↓               ↓              ↓             ↓            ↓
Temperature → MQTT Messages → Kafka Stream → Processing → Real-time → Color Changes
```

## 🚀 Quick Start

### Prerequisites
1. **Configure credentials:**
   ```bash
   cp config.template.py config.py
   # Edit config.py with your Azure credentials
   ```

2. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

### Running the System

**Terminal 1** - Start IoT Temperature Simulator:
```bash
python iot_temperature_simulator.py
```

**Terminal 2** - Start Kafka Consumer & WebSocket Bridge:
```bash
python ultra_minimal_kafka_consumer.py
```

**Terminal 3** - Start Web Server:
```bash
python serve_usd_viewer.py
```

**Browser** - Open the 3D Viewer:
```
http://localhost:8080/simple_usd_viewer.html
```

### Expected Flow
1. IoT simulator sends temperature data to Azure IoT Hub every 10 seconds
2. Azure Event Hub receives the data via Kafka streaming
3. Python consumer processes messages and broadcasts via WebSocket
4. 3D viewer updates rack colors based on temperature thresholds

## 🧪 Testing

The system automatically generates temperature data from `rack_temperatures_clean.csv`. You should see:

- **Console logs** showing temperature readings being sent to Azure IoT Hub
- **Kafka messages** being processed by the consumer 
- **WebSocket connections** in the browser console
- **Color changes** in the 3D viewer when temperatures exceed thresholds

## 📡 Azure Integration

### IoT Hub (MQTT)
- **Protocol:** MQTT over TLS
- **Message Format:** JSON with rack path and temperature
- **Frequency:** Every 10 seconds

### Event Hub (Kafka)
- **Protocol:** SASL_SSL with PLAIN authentication
- **Consumer Pattern:** Ultra-minimal configuration for Azure compatibility
- **Message Processing:** Temperature → Status → Color mapping

### Temperature Thresholds
- **> 25°C:** ERROR (Red)
- **> 20°C:** WARNING (Orange)  
- **≤ 20°C:** OK (Green for Rack1, Blue for Rack2)

### Processed Message Format
```json
{
  "type": "rack_update",
  "rack_id": "Rack1", 
  "status": "ERROR",
  "color": "0xFF0000",
  "temperature": 26.5,
  "timestamp": "2025-10-27T14:30:00"
}
```

## 📁 Project Files

### Core Components
- **`iot_temperature_simulator.py`** - Simulates IoT device sending temperature data to Azure IoT Hub via MQTT
- **`ultra_minimal_kafka_consumer.py`** - Azure Event Hub Kafka consumer with WebSocket broadcasting
- **`simple_usd_viewer.html`** - 3D visualization using Three.js with real-time rack color updates
- **`serve_usd_viewer.py`** - HTTP server for serving the web viewer locally

### Data Files
- **`rack_temperatures_clean.csv`** - Sample temperature data for 2 racks with realistic values
- **`racks.usd`** - USDA 3D model definition containing Rack1 and Rack2 geometry

### Configuration
- **`config.py`** - Contains all Azure credentials and configuration (not committed to git)
- **`config.template.py`** - Template file showing required configuration structure
- **`requirements.txt`** - Python package dependencies
- **`.gitignore`** - Git ignore rules (includes config.py for security)

### Purpose of Each File:

#### `iot_temperature_simulator.py`
- Reads temperature data from CSV file
- Sends JSON messages to Azure IoT Hub every 10 seconds
- Uses MQTT protocol with Azure IoT Device SDK
- Simulates real IoT device behavior with rack path and temperature

#### `ultra_minimal_kafka_consumer.py` 
- Connects to Azure Event Hub using Kafka protocol
- Uses ultra-minimal configuration for Azure compatibility
- Processes temperature messages and determines status (OK/WARNING/ERROR)
- Broadcasts rack updates to web clients via WebSocket on port 8767
- Thread-safe message queue prevents asyncio conflicts

#### `simple_usd_viewer.html`
- Custom Three.js 3D viewer with USDA file parser
- Renders Rack1 (green) and Rack2 (blue) as colored cubes
- WebSocket client receives real-time updates
- Updates rack colors based on temperature status
- Interactive controls: rotate, zoom, wireframe toggle

#### `serve_usd_viewer.py`
- Simple HTTP server for local development
- Serves HTML, USDA files, and other static content
- Enables CORS for local file access
- Runs on port 8080

## 🎮 Web Viewer Controls

- **Mouse drag:** Rotate view
- **Mouse scroll:** Zoom in/out
- **Reset View:** Return to original position
- **Toggle Wireframe:** Switch between solid/wireframe view
- **Toggle Rotation:** Enable/disable auto-rotation

## 🔧 Configuration

### Required Setup
1. Copy `config.template.py` to `config.py`
2. Fill in your Azure credentials:
   - IoT Hub connection string
   - Event Hub connection string
   - Consumer group and topic name

### Network Ports
- **HTTP Server:** 8080 (web viewer)
- **WebSocket:** 8767 (real-time updates)

### Azure Resources Needed
- **Azure IoT Hub** - For MQTT device connectivity
- **Azure Event Hub** - For Kafka streaming (must support Kafka protocol)
- **Service Bus Namespace** - Container for Event Hub

## 🌟 Features

- ✅ Real-time 3D visualization
- ✅ Kafka streaming integration
- ✅ WebSocket communication
- ✅ Status-based color changes
- ✅ Interactive 3D controls
- ✅ Connection auto-recovery
- ✅ Comprehensive logging
- ✅ Azure IoT Hub integration

## 🚨 Troubleshooting

### Common Issues

#### "Config module not found"
```bash
cp config.template.py config.py
# Edit config.py with your Azure credentials
```

#### WebSocket Connection Issues
- Ensure `ultra_minimal_kafka_consumer.py` is running
- Check port 8767 is not blocked by firewall
- Verify WebSocket connection in browser console

#### Kafka Connection Issues  
- Azure Event Hub requires ultra-minimal configuration
- Check Service Bus namespace and Event Hub name
- Verify SASL_SSL authentication credentials
- Network connectivity to `*.servicebus.windows.net:9093`

#### 3D Viewer Issues
- Ensure HTTP server is running: `python serve_usd_viewer.py`
- Open `http://localhost:8080/simple_usd_viewer.html` (not file://)
- Check browser console for JavaScript errors
- Verify USDA file loading and WebSocket connection

#### No Color Changes
- Check IoT simulator is sending data (console logs)
- Verify Kafka consumer is processing messages
- Confirm WebSocket connection in browser
- Temperature thresholds: >25°C (red), >20°C (orange), ≤20°C (normal)

### Monitoring Tips
- IoT simulator should show "Message sent successfully" every 10 seconds
- Kafka consumer should show "📩 Message X" and "🎨 RackY: temperature -> status"  
- Browser console should show WebSocket connected and incoming messages
- 3D viewer should display color changes when temperature thresholds are exceeded