import asyncio
import argparse
import random
import time
from datetime import datetime
from bleak import BleakClient, BleakScanner
from supabase import create_client, Client

from config import SUPABASE_KEY, SUPABASE_URL, CHARACTERISTICS, TEMPERATURE_BYTE, BATTERY_BYTE, HUMDITY_BYTE, PRESSURE_BYTE

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Find sensor function
async def find_sensor(sensor_name):
    """Scans for BLE devices and returns the first match for the given name."""
    print("Scanning for devices...")
    devices = await BleakScanner.discover()

    for device in devices:
        if device.name and sensor_name in device.name:
            print(f"Found sensor: {device.name}")
            return device

    print(f"Sensor with name containing '{sensor_name}' not found.")
    return None

# Write to Supabase
def write_to_supabase(data):
    """Writes a row of sensor data to the Supabase table."""
    payload = {
        "timestamp": datetime.now().isoformat(),
        "temperature_c": data.get("Temperature (°C)"),
        "humidity_percent": data.get("Relative Humidity (%)"),
        "pressure_pa": data.get("Barometric Pressure (Pa)"),
        "battery_voltage_mv": data.get("Battery Voltage (mV)")
            if isinstance(data.get("Battery Voltage (mV)"), (int, float))
            else float(data.get("Battery Voltage (mV)").split()[0]) * 1000
            if data.get("Battery Voltage (mV)") else None,
        "device_name": data.get("device_name")
    }

    supabase.table("sensor_data").insert(payload).execute()
    print("Data written to Supabase")

async def read_sensor_data(sensor_name):
    """Reads real data from the BLE sensor and writes it to Supabase every 5 seconds."""
    sensor_device = await find_sensor(sensor_name)
    if not sensor_device:
        return

    async with BleakClient(sensor_device) as client:
        print(f"Connected to {sensor_device.name}.")

        while True:
            results = {}
            for char_uuid, description in CHARACTERISTICS.items():
                try:
                    # Trigger a new reading if needed
                    if char_uuid in CHARACTERISTICS.keys():
                        write_value = bytearray([0x01, 0x00, 0x00, 0x00])
                        await client.write_gatt_char(char_uuid, write_value)

                    # Read the characteristic value
                    data = await client.read_gatt_char(char_uuid)

                    # Process based on type
                    if char_uuid == TEMPERATURE_BYTE:
                        value = int.from_bytes(data, byteorder="little", signed=True) / 100
                    elif char_uuid == HUMDITY_BYTE:
                        value = int.from_bytes(data, byteorder="little", signed=True) / 100
                    elif char_uuid == PRESSURE_BYTE:
                        value = int.from_bytes(data, byteorder="little", signed=False) / 100
                    elif char_uuid == BATTERY_BYTE:
                        battery_voltage = (
                            int.from_bytes(data[:2], byteorder="little", signed=False) / 1000
                        )
                        temperature = (
                            int.from_bytes(data[2:], byteorder="little", signed=True) / 100
                        )
                        value = f"{battery_voltage:.3f} V (Temperature: {temperature:.2f}°C)"
                    else:
                        value = data.hex()

                    results[description] = value
                except Exception as e:
                    print(f"Failed to read {description}: {e}")

            results["device_name"] = sensor_device.name
            print(f"[{datetime.now().isoformat()}] Sensor Data: {results}")
            write_to_supabase(results)

            await asyncio.sleep(5)

def simulate_sensor_data():
    """Simulates sensor data readings and writes to Supabase every 5 seconds."""
    print("Simulating sensor data... (Press Ctrl+C to stop)")
    try:
        while True:
            data = {
                "Temperature (°C)": round(random.uniform(15, 30), 2),
                "Relative Humidity (%)": round(random.uniform(40, 70), 2),
                "Barometric Pressure (Pa)": round(random.uniform(99000, 102000), 2),
                "Battery Voltage (mV)": round(random.uniform(3.5, 4.2), 3),
                "device_name": "SimulatedSensor-1"
            }
            print(f"[{datetime.now().isoformat()}] Simulated Data: {data}")
            write_to_supabase(data)
            time.sleep(5)
    except KeyboardInterrupt:
        print("Simulation stopped.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="CLI for reading or simulating sensor data.")
    parser.add_argument("--simulate", action="store_true", help="Run in simulation mode instead of reading real sensor data.")
    args = parser.parse_args()

    if args.simulate:
        simulate_sensor_data()
    else:
        sensor_name = "SensorPush HTP.xw DD6"  # Adjust as needed
        asyncio.run(read_sensor_data(sensor_name))
