import asyncio
import argparse
import random
import time
import csv
import os
from datetime import datetime
from bleak import BleakClient, BleakScanner

CHARACTERISTICS = {
    "EF090080-11D6-42BA-93B8-9DD7EC090AA9": "Temperature (°C)",
    "EF090081-11D6-42BA-93B8-9DD7EC090AA9": "Relative Humidity (%)",
    "EF090082-11D6-42BA-93B8-9DD7EC090AA9": "Barometric Pressure (Pa)",
    "EF090007-11D6-42BA-93B8-9DD7EC090AA9": "Battery Voltage (mV)",
}


def get_csv_filename():
    """Generates a CSV filename based on the current date and hour."""
    now = datetime.now()
    return f"sensor_data_{now.strftime('%Y-%m-%d_%H')}.csv"


def write_to_csv(data):
    """Writes sensor data to a CSV file for the current hour."""
    filename = get_csv_filename()
    file_exists = os.path.isfile(filename)

    with open(filename, mode="a", newline="") as file:
        writer = csv.writer(file)

        # Write header if the file is new
        if not file_exists:
            writer.writerow(["Timestamp"] + list(data.keys()))

        # Write the data row
        writer.writerow([datetime.now().isoformat()] + list(data.values()))


async def read_sensor_data(sensor_name):
    """Reads real data from the BLE sensor and writes it to a CSV file."""
    print("Scanning for devices...")
    devices = await BleakScanner.discover()

    sensor_device = next((d for d in devices if d.name and sensor_name in d.name), None)

    if not sensor_device:
        print(f"Sensor with name containing '{sensor_name}' not found.")
        return

    print(f"Found sensor: {sensor_device.name}")
    async with BleakClient(sensor_device) as client:
        print("Connected to the sensor.")
        results = {}

        for char_uuid, description in CHARACTERISTICS.items():
            try:
                # Trigger a new reading for sensor values
                if char_uuid in CHARACTERISTICS.keys():
                    write_value = bytearray([0x01, 0x00, 0x00, 0x00])
                    await client.write_gatt_char(char_uuid, write_value)

                # Read the characteristic value
                data = await client.read_gatt_char(char_uuid)

                # Process data based on characteristic type
                if char_uuid == "EF090080-11D6-42BA-93B8-9DD7EC090AA9":  # Temperature
                    value = int.from_bytes(data, byteorder="little", signed=True) / 100
                elif char_uuid == "EF090081-11D6-42BA-93B8-9DD7EC090AA9":  # Humidity
                    value = int.from_bytes(data, byteorder="little", signed=True) / 100
                elif char_uuid == "EF090082-11D6-42BA-93B8-9DD7EC090AA9":  # Pressure
                    value = int.from_bytes(data, byteorder="little", signed=False) / 100
                elif char_uuid == "EF090007-11D6-42BA-93B8-9DD7EC090AA9":  # Battery
                    battery_voltage = (
                        int.from_bytes(data[:2], byteorder="little", signed=False)
                        / 1000
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

        print("\nSensor Readings:")
        for desc, val in results.items():
            print(f"{desc}: {val}")

        # Write the data to CSV
        write_to_csv(results)
        print(f"Data written to {get_csv_filename()}")


def simulate_sensor_data():
    """Simulates sensor data readings at the same cadence and writes to a CSV file."""
    print("\nSimulating sensor data... (Press Ctrl+C to stop)\n")
    try:
        while True:
            simulated_data = {
                "Temperature (°C)": round(random.uniform(15, 30), 2),
                "Relative Humidity (%)": round(random.uniform(40, 70), 2),
                "Barometric Pressure (Pa)": round(random.uniform(99000, 102000), 2),
                "Battery Voltage (mV)": round(random.uniform(3.5, 4.2), 3),
            }

            print(f"[{datetime.now().isoformat()}] Simulated Data:")
            for key, value in simulated_data.items():
                print(f"{key}: {value}")

            print("\n---\n")

            # Write simulated data to CSV
            write_to_csv(simulated_data)
            print(f"Simulated data written to {get_csv_filename()}")

            time.sleep(10)  # Simulating data every 10 seconds
    except KeyboardInterrupt:
        print("Simulation stopped.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="CLI for reading or simulating sensor data.")
    parser.add_argument(
        "--simulate", action="store_true", help="Run in simulation mode instead of reading real sensor data."
    )
    args = parser.parse_args()

    if args.simulate:
        simulate_sensor_data()
    else:
        sensor_name = "SensorPush HTP.xw DD6"  # Adjust sensor name as needed
        asyncio.run(read_sensor_data(sensor_name))

