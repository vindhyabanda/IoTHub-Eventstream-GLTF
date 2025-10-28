#!/usr/bin/env python3
"""
Azure Event Hub Consumer
Uses the absolute bare minimum configuration
"""

import json
import asyncio
import websockets
import logging
import threading
import time
import queue
from datetime import datetime
from kafka import KafkaConsumer
from config import EVENT_HUB_CONNECTION_STRING, CONSUMER_GROUP, TOPIC_NAME, WEBSOCKET_PORT

# Configuration - absolute minimum
namespace = EVENT_HUB_CONNECTION_STRING.split("://")[1].split(".")[0]
bootstrap_server = f"{namespace}.servicebus.windows.net:9093"

# Ultra-minimal config for Azure Event Hub compatibility
KAFKA_CONFIG = {
    'bootstrap_servers': [bootstrap_server],
    'security_protocol': 'SASL_SSL',
    'sasl_mechanism': 'PLAIN',
    'sasl_plain_username': '$ConnectionString',
    'sasl_plain_password': EVENT_HUB_CONNECTION_STRING,
    'group_id': CONSUMER_GROUP,
    'client_id': 'PythonKafkaConsumer',
    'auto_offset_reset': 'latest',  # Only get new messages
    'value_deserializer': lambda x: json.loads(x.decode('utf-8')),
    
    # Minimal timeouts
    'session_timeout_ms': 30000,
    'heartbeat_interval_ms': 10000,
    'request_timeout_ms': 40000,
    
    # Critical: NO fetch configuration - let Azure decide
    # Remove all fetch_* parameters that might conflict
}

connected_clients = set()
message_queue = queue.Queue()

logging.basicConfig(level=logging.WARNING)  # Reduce log noise

def process_message(message):
    """Process OTEL message and broadcast formatted rack updates to WebSocket"""
    try:
        # Show complete Kafka message structure
        print(f"📩 === FULL KAFKA MESSAGE ===")
        print(f"📍 Topic: {message.topic}")
        print(f"📍 Partition: {message.partition}")
        print(f"📍 Offset: {message.offset}")
        print(f"📍 Timestamp: {message.timestamp}")
        print(f"📍 Headers: {message.headers}")
        print(f"📍 Key: {message.key}")
        
        # The value is already parsed as JSON by the deserializer
        parsed_data = message.value
        print(f"📩 Parsed Value: {json.dumps(parsed_data, indent=2)}")
        
        # Check if this is OTEL format
        if isinstance(parsed_data, dict):
            if 'resourceLogs' in parsed_data:
                print(f"🔍 OTEL Format Detected - Resource Logs")
                otel_data = parsed_data
            elif 'resourceSpans' in parsed_data:
                print(f"🔍 OTEL Format Detected - Resource Spans")
                otel_data = parsed_data
            elif 'resourceMetrics' in parsed_data:
                print(f"🔍 OTEL Format Detected - Resource Metrics")
                otel_data = parsed_data
            else:
                print(f"🔍 Custom Format - Using direct data")
                otel_data = parsed_data
        else:
            print(f"🔍 Data Format: {type(parsed_data)}")
            otel_data = parsed_data
        
        # Extract data for processing (handle both OTEL and custom formats)
        data_list = []
        
        # Handle OTEL Resource Logs format
        if 'resourceLogs' in otel_data:
            for resource_log in otel_data.get('resourceLogs', []):
                for scope_log in resource_log.get('scopeLogs', []):
                    for log_record in scope_log.get('logRecords', []):
                        # Extract attributes from OTEL log record
                        attributes = {}
                        for attr in log_record.get('attributes', []):
                            key = attr.get('key', '')
                            value = attr.get('value', {})
                            if 'stringValue' in value:
                                attributes[key] = value['stringValue']
                            elif 'doubleValue' in value:
                                attributes[key] = value['doubleValue']
                            elif 'intValue' in value:
                                attributes[key] = value['intValue']
                        
                        # Try to extract temperature and path from OTEL attributes or body
                        data_item = {
                            'path': attributes.get('path', '/World/Rack1/'),
                            'temperature': float(attributes.get('temperature', 20.0)),
                            'status': attributes.get('status', 'OK'),
                            'otel_timestamp': log_record.get('timeUnixNano'),
                            'otel_body': log_record.get('body', {})
                        }
                        data_list.append(data_item)
        
        # Handle direct data format (your current messages)
        elif isinstance(otel_data, list):
            data_list = otel_data
        else:
            data_list = [otel_data]
        
        # Process each data item
        for data in data_list:
            # Extract rack information
            path = data.get('path', '/World/Rack1/')
            temperature = float(data.get('temperature', 20.0))
            status = data.get('status', 'OK')
            
            # Determine rack ID
            if 'Rack1' in path:
                rack_id = 'Rack1'
            elif 'Rack2' in path:
                rack_id = 'Rack2'
            else:
                rack_id = 'Rack1'
            
            # Determine color based on status and temperature
            if status == 'ERROR' or temperature > 22.0:
                color = '0xFF0000'  # Red
                final_status = 'ERROR'
            elif temperature > 20.0:
                color = '0xFFA500'  # Orange
                final_status = 'WARNING'
            else:
                color = '0x4CAF50' if rack_id == 'Rack1' else '0x2196F3'  # Green/Blue
                final_status = 'OK'
            
            # Create rack update message for 3D viewer
            rack_update = {
                'type': 'rack_update',
                'rack_id': rack_id,
                'status': final_status,
                'color': color,
                'temperature': temperature,
                'timestamp': datetime.now().isoformat()
            }
            
            print(f"🎨 {rack_id}: {temperature}°C -> {final_status} ({color})")
            
            # Put message in queue for WebSocket handler to send
            message_queue.put(json.dumps(rack_update))
        
    except Exception as e:
        print(f"❌ Message processing error: {e}")

async def broadcast_to_clients(message):
    """Broadcast message to all connected clients"""
    if not connected_clients:
        return
    
    clients_copy = connected_clients.copy()
    for client in clients_copy:
        try:
            await client.send(message)
        except:
            connected_clients.discard(client)

async def websocket_handler(websocket):
    """Handle WebSocket connections"""
    print(f"🔗 WebSocket connected from {websocket.remote_address}")
    connected_clients.add(websocket)
    
    try:
        while True:
            # Check for new messages to send
            try:
                message = message_queue.get_nowait()
                await websocket.send(message)
            except queue.Empty:
                pass
            except:
                break
            
            # Wait a bit before checking again
            await asyncio.sleep(0.1)
            
            # Check if websocket is still alive
            try:
                await websocket.ping()
            except:
                break
                
    except:
        pass
    finally:
        connected_clients.discard(websocket)
        print(f"👋 WebSocket disconnected. Active: {len(connected_clients)}")

def minimal_consumer():
    print("🚀 Starting Azure Event Hub consumer...")
    print(f"📡 Server: {bootstrap_server}")
    print(f"📋 Topic: {TOPIC_NAME}")
    print(f"👥 Group: {CONSUMER_GROUP}")
    print(f"🔑 Connection String Length: {len(EVENT_HUB_CONNECTION_STRING)} chars")
    print(f"🔑 Connection String Preview: {EVENT_HUB_CONNECTION_STRING[:50]}...")
    
    while True:
        try:
            print("\n🔌 Creating minimal consumer...")
            
            # Create consumer with absolute minimum config
            consumer = KafkaConsumer(
                TOPIC_NAME,
                **KAFKA_CONFIG
            )
            
            print("✅ Consumer created!")
            print("📡 Waiting for messages...")
            
            # Use the most basic consumption method
            message_count = 0
            for message in consumer:
                message_count += 1
                print(f"\n📩 === Processing Message {message_count} ===")
                process_message(message)
                print(f"📩 === End Message {message_count} ===\n")
                
        except Exception as e:
            print(f"❌ Consumer error: {e}")
            try:
                consumer.close()
            except:
                pass
            
            print("⏳ Retrying in 10 seconds...")
            time.sleep(10)

async def main():
    """Main function"""
    print("🚀 Kafka to WebSocket bridge")
    
    # Start consumer thread
    kafka_thread = threading.Thread(target=minimal_consumer, daemon=True)
    kafka_thread.start()
    
    # Start WebSocket server
    print(f"🌐 WebSocket server on port {WEBSOCKET_PORT}")
    server = await websockets.serve(websocket_handler, "localhost", WEBSOCKET_PORT)
    
    print("✅ Bridge running!")
    print(f"🔌 WebSocket: ws://localhost:{WEBSOCKET_PORT}")
    
    await server.wait_closed()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑 Stopped")
    except Exception as e:
        print(f"❌ Error: {e}")