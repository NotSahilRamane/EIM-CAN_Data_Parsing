import json
import time
import cantools
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import threading

PARSED_LOG_FILE = "mqtt_parsed_log.txt"

try:
    dbc_all = cantools.database.load_file(r"C:\Users\srama\OneDrive\Desktop\Telematics_Data\LonaMa_DBC_BFDA_CEV_VCAN_Matrix_CAN_V1.0.dbc")
    dbc_j1939 = cantools.database.load_file(r"C:\Users\srama\OneDrive\Desktop\Telematics_Data\j1939.dbc")
    print("DBC files loaded successfully.")
except Exception as e:
    print(f"Error loading DBC files: {e}")
    exit(1)

log_lock = threading.Lock()  # Lock for writing to file

def log_parsed_data(timestamp, can_id, parsed_data):
    """Logs parsed CAN data to a text file safely using a lock."""
    with log_lock:
        with open(PARSED_LOG_FILE, "a") as f:
            f.write(f"{timestamp}, ID={hex(can_id)}, Parsed={parsed_data}\n")

def parse_can_message(can_id, data):
    """Parses CAN data using the appropriate DBC file."""
    try:
        if can_id == 0x18FEC1FE:
            message = dbc_j1939.get_message_by_frame_id(can_id)
        else:
            message = dbc_all.get_message_by_frame_id(can_id)

        parsed_data = message.decode(bytes.fromhex(data))
        return parsed_data
    except Exception as e:
        return f"Parsing error for ID {hex(can_id)}: {e}"

def process_line(line):
    """Processes a single line of CAN log data."""
    try:
        timestamp, can_id, data = line.strip().split(", ")
        timestamp = float(timestamp)
        can_id = int(can_id.split("=")[1], 16)
        data = data.split("=")[1]
        parsed_data = parse_can_message(can_id, data)
        if parsed_data:
            log_parsed_data(timestamp, can_id, parsed_data)
    except Exception as e:
        print(f"Error parsing data for line {line}: {e}")

def main():
    with open(r"C:\Users\srama\OneDrive\Desktop\Telematics_Data\Data to Use\can_log.txt", "r") as f:
        lines = f.readlines()

    with ThreadPoolExecutor(max_workers=4) as executor:  # Reduced threads for efficiency
        futures = []
        
        with tqdm(total=len(lines), desc="Processing", unit="lines") as pbar:
            for line in lines:
                futures.append(executor.submit(process_line, line))
                if len(futures) > 3000:  # Limit the number of tasks in memory
                    for future in as_completed(futures):
                        try:
                            future.result()
                        except Exception as e:
                            print(f"Error processing line: {e}")
                        pbar.update(1)
                    futures = []  # Clear completed futures

            # Process remaining futures
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    print(f"Error processing line: {e}")
                pbar.update(1)

if __name__ == "__main__":
    main()
