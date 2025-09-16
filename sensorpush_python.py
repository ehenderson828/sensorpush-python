import asyncio
import argparse
import random
import time
import csv
import os
import logging
import sys
import atexit
from datetime import datetime
from bleak import BleakClient, BleakScanner
from supabase import create_client, Client

from config import SUPABASE_KEY, SUPABASE_URL, CHARACTERISTICS, TEMPERATURE_BYTE, BATTERY_BYTE, HUMDITY_BYTE, PRESSURE_BYTE

# Single instance check
LOCKFILE = "/tmp/sensor_script.lock"

def remove_lock():
    if os.path.exists(LOCKFILE):
        os.remove(LOCKFILE)

if os.path.exists(LOCKFILE):
    try:
        with open(LOCKFILE, "r") as f:
            pid = int(f.read())
        os.kill(pid, 0)  # Check if process is still running
        print("Another instance of the script is already running. Exiting.")
        sys.exit(1)
    except ValueError:
        print("Lockfile corrupted. Removing stale lockfile.")
        os.remove(LOCKFILE)
    except ProcessLookupError:
        print("Stale lockfile found. Removing it.")
        os.remove(LOCKFILE)
    except PermissionError:
        print(f"Cannot check PID {pid}. Exiting for safety.")
        sys.exit(1)

with open(LOCKFILE, "w") as f:
    f.write(str(os.getpid()))

atexit.register(remove_lock)
# --- End of lockfile logic ---

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('sensor.log'),
        logging.StreamHandler()  # Also output to console
    ]
)
logger = logging.getLogger(__name__)

# Supabase client configuration
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def filter_sensor_data(data: dict) -> dict:
    """Remove None or empty values from sensor data before writing."""
    return {k: v for k, v in data.items() if v not in (None, "", "NaN")}

# Find sensor function
async def find_sensor(sensor_name):
    """Scans for BLE devices and returns the first match for the given name."""
    logger.info("Scanning for devices...")

    devices = await BleakScanner.discover()

    for device in devices:
        if device.name and sensor_name in device.name:
            logger.info(f"Found sensor: {device.name}")
            return device

    logger.warning(f"Sensor with name containing '{sensor_name}' not found.")
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

    try:
        response = (
            supabase.table("sensor_data")
            .insert(payload)
            .execute()
        )
        logger.info("Data written to Supabase successfully")
    except Exception as e:
        logger.error(f"Failed to write data to Supabase: {e}")

def get_csv_filename():
    """Generates a CSV filename based on the current date and hour."""

    if not os.path.exists('data'):
        os.mkdir('data')
        logger.info("Created 'data' directory")

    now = datetime.now()
    return f"data/sensor_data_{now.strftime('%Y-%m-%d_%H')}.csv"
 

def write_data(data, args):
    if args.local:
        # Write data to CSV
        write_to_csv(data)
        logger.info(f"Data written to {get_csv_filename()}")
    else:
        # Write data to Supabase
        write_to_supabase(data)


def write_to_csv(data):
    """Writes sensor data to a CSV file for the current hour."""
    filename = get_csv_filename()
    file_exists = os.path.isfile(filename)

    try:
        with open(filename, mode="a", newline="") as file:
            writer = csv.writer(file)

            # Write header if the file is new
            if not file_exists:
                writer.writerow(["Timestamp"] + list(data.keys()))
                logger.info(f"Created new CSV file: {filename}")

            # Write the data row
            writer.writerow([datetime.now().isoformat()] + list(data.values()))
    except Exception as e:
        logger.error(f"Failed to write to CSV file {filename}: {e}")


async def read_sensor_data(sensor_name, args, retries=5, delay=10):
    """Reads real data from the BLE sensor and writes it to a CSV file."""
    sensor_device = await find_sensor(sensor_name)
    if not sensor_device:
        logger.error("No sensor device found, exiting")
        return

    logger.info(f"Found sensor: {sensor_device.name}")

    attempt = 0
    while attempt < retries:
        try:
            async with BleakClient(sensor_device) as client:
                logger.info("Connected to the sensor")
                results = {}

                for char_uuid, description in CHARACTERISTICS.items():
                    try:
                        # Trigger a new reading
                        if char_uuid in CHARACTERISTICS.keys():
                            write_value = bytearray([0x01, 0x00, 0x00, 0x00])
                            await client.write_gatt_char(char_uuid, write_value)

                        # Read the characteristic value
                        data = await client.read_gatt_char(char_uuid)

                        # Process data based on characteristic type
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
                        logger.debug(f"Read {description}: {value}")
                    except Exception as e:
                        logger.error(f"Failed to read {description}: {e}")

                results["device_name"] = sensor_device.name

                # Filter null/invalid values before writing
                cleaned_results = filter_sensor_data(results)

                if cleaned_results:
                    write_data(cleaned_results, args)
                else:
                    logger.warning("Skipping write: no valid sensor data found")

                return  # Success — exit retry loop

        except (BrokenPipeError, EOFError, Exception) as e:
            attempt += 1
            logger.error(f"Error on attempt {attempt}/{retries}: {e}")
            if attempt < retries:
                logger.info(f"Retrying in {delay} seconds...")
                await asyncio.sleep(delay)
            else:
                logger.error("Max retries reached, giving up on this cycle")

def simulate_sensor_data(args):
    """Simulates sensor data readings at the same cadence and writes to a CSV file."""
    logger.info("Starting sensor data simulation... (Press Ctrl+C to stop)")
    try:
        while True:
            data = {
                "Temperature (°C)": round(random.uniform(15, 30), 2),
                "Relative Humidity (%)": round(random.uniform(40, 70), 2),
                "Barometric Pressure (Pa)": round(random.uniform(99000, 102000), 2),
                "Battery Voltage (mV)": round(random.uniform(3.5, 4.2), 3),
            }

            # Add simulated device name
            data["device_name"] = "SimulatedSensor-1"

            logger.info("Simulated Data:")
            for key, value in data.items():
                logger.info(f"  {key}: {value}")

            logger.info("---")

            # Filter null/invalid values before writing
            cleaned_data = filter_sensor_data(data)

            if cleaned_data:
                write_data(cleaned_data, args)
            else:
                logger.warning("Skipping write: no valid simulated data")

            time.sleep(10)  # Simulating data every 10 seconds
    except KeyboardInterrupt:
        logger.info("Simulation stopped by user")
    except Exception as e:
        logger.error(f"Error during simulation: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="CLI for reading or simulating sensor data.")
    parser.add_argument(
        "--simulate", action="store_true", help="Run in simulation mode instead of reading real sensor data."
    )
    parser.add_argument(
        "--local", action="store_true", help="Run in local mode instead of writing data.", default=False
    )
    args = parser.parse_args()

    logger.info("Starting sensor script")
    logger.info(f"Arguments: simulate={args.simulate}, local={args.local}")

    try:
        if args.simulate:
            simulate_sensor_data(args)
        else:
            sensor_name = "SensorPush HTP.xw DD6"  # Adjust sensor name as needed
            while True:
                asyncio.run(read_sensor_data(sensor_name, args))
                logger.info("Sleeping 15 minutes before next reading...")
                time.sleep(15 * 60) # 15 minute sleep time
    except Exception as e:
        logger.error(f"Fatal error: {e}")
    finally:
        logger.info("Sensor script finished")