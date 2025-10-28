# Configuration template file
# Copy this to config.py and fill in your actual credentials

# Azure IoT Hub Configuration
IOT_HUB_CONNECTION_STRING = "HostName=YOUR_IOT_HUB.azure-devices.net;DeviceId=YOUR_DEVICE_ID;SharedAccessKey=YOUR_SHARED_ACCESS_KEY"

# Azure Event Hub (Kafka) Configuration
EVENT_HUB_CONNECTION_STRING = "Endpoint=sb://YOUR_NAMESPACE.servicebus.windows.net/;SharedAccessKeyName=YOUR_KEY_NAME;SharedAccessKey=YOUR_SHARED_ACCESS_KEY;EntityPath=YOUR_ENTITY_PATH"
CONSUMER_GROUP = "YOUR_CONSUMER_GROUP"
TOPIC_NAME = "YOUR_TOPIC_NAME"

# WebSocket Configuration
WEBSOCKET_PORT = 8767