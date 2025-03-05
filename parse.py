
import json
import time
import cantools

PARSED_LOG_FILE = "mqtt_parsed_log.txt"


try:
    dbc_all = cantools.database.load_file(r"C:\Users\srama\OneDrive\Desktop\Telematics_Data\LonaMa_DBC_BFDA_CEV_VCAN_Matrix_CAN_V1.0.dbc")  # General IDs
    dbc_j1939 = cantools.database.load_file(r"C:\Users\srama\OneDrive\Desktop\Telematics_Data\j1939.dbc")  # Specific J1939 ID
    print("DBC files loaded successfully.")
except Exception as e:
    print(f"Error loading DBC files: {e}")
    exit(1)


def log_parsed_data(timestamp, can_id, parsed_data):
    """Logs parsed CAN data to a text file."""
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
        print(f"Parsing error for ID {hex(can_id)}: {e}")
        return None


with open(r"C:\Users\srama\OneDrive\Desktop\Telematics_Data\Data to Use\can_log.txt", "r") as f:
    lines = f.readlines()
    for line in lines:
        timestamp, can_id, data = line.strip().split(", ")
        try:
            timestamp = float(timestamp)
            can_id = int(can_id.split("=")[1], 16)
            data = data.split("=")[1]
            parsed_data = parse_can_message(can_id, data)
        except Exception as e:
            print(f"Error parsing data for ID {hex(can_id)}: {e}")
            # skip to the next line
            parsed_data = None
        if parsed_data:
            log_parsed_data(timestamp, can_id, parsed_data)
            print(f"Parsed: {parsed_data}")
