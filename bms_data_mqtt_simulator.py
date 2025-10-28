#!/usr/bin/env python3
"""
Custom BMS Data MQTT Simulator
Simulates an IoT device sending BMS data to Azure IoT Hub over MQTT.
"""

import csv
import json
import time
import asyncio
from azure.iot.device.aio import IoTHubDeviceClient
from azure.iot.device import Message
from config import IOT_HUB_CONNECTION_STRING

# CSV file path
CSV_FILE = "nvidia_bms_data.csv"

async def main():
    """Main function to simulate IoT device sending temperature data."""
    
    print("Starting BMS Data MQTT Simulator...")
    
    # Create IoT Hub device client
    client = None
    try:
        client = IoTHubDeviceClient.create_from_connection_string(IOT_HUB_CONNECTION_STRING)
        print("IoT Hub client created successfully")
    except Exception as e:
        print(f"Error creating IoT Hub client: {e}")
        return
    
    try:
        # Connect to IoT Hub
        await client.connect()
        print("Connected to IoT Hub")
        
        # Read CSV file and send messages
        message_count = 0
        
        with open(CSV_FILE, 'r', newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            
            for row in reader:
                try:
                    # Create JSON message
                    message_data = {
                        "path": row['topic_path'],
                        "point_type": row['point_type'],
                        "temperature": float(row['average_value'])
                    }
                    
                    # Convert to JSON string
                    message_json = json.dumps(message_data)
                    
                    # Print message before sending
                    print(f"Sending message {message_count + 1}: {message_json}")
                    
                    # Create IoT Hub message
                    message = Message(message_json)
                    message.content_type = "application/json"
                    message.content_encoding = "utf-8"
                    
                    # Send message to IoT Hub
                    await client.send_message(message)
                    print(f"Message {message_count + 1} sent successfully")
                    message_count += 1
                    
                    # Wait 10 seconds before sending next message
                    await asyncio.sleep(10)
                    
                except asyncio.CancelledError:
                    print("\nSimulation interrupted by user")
                    break
                except Exception as e:
                    print(f"Error sending message {message_count + 1}: {e}")
        
        print(f"Simulation complete. Total messages sent: {message_count}")
        
    except FileNotFoundError:
        print(f"Error: CSV file '{CSV_FILE}' not found")
    except KeyboardInterrupt:
        print("\nSimulation interrupted by user")
    except Exception as e:
        print(f"Error during simulation: {e}")
    
    finally:
        # Close the connection
        if client:
            try:
                await client.disconnect()
                print("Disconnected from IoT Hub")
            except Exception as e:
                print(f"Error disconnecting from IoT Hub: {e}")

if __name__ == "__main__":
    # Run the async main function
    asyncio.run(main())