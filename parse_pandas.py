import json
import time
import cantools
import pandas as pd

PARSED_LOG_FILE = "mqtt_parsed_log111.txt"

# Load DBC files
try:
    dbc_all = cantools.database.load_file(r"C:\Users\srama\OneDrive\Desktop\Telematics_Data\LonaMa_DBC_BFDA_CEV_VCAN_Matrix_CAN_V1.0.dbc")
    dbc_j1939 = cantools.database.load_file(r"C:\Users\srama\OneDrive\Desktop\Telematics_Data\j1939.dbc")
    print("DBC files loaded successfully.")
except Exception as e:
    print(f"Error loading DBC files: {e}")
    exit(1)

def parse_can_message(row):
    """Parses CAN data using the appropriate DBC file."""
    try:
        # print(row["can_id"], "CAN ID")
        if row["can_id"] == 0x18FEC1FE:
            message = dbc_j1939.get_message_by_frame_id(row["can_id"])
        else:
            message = dbc_all.get_message_by_frame_id(row["can_id"])

        parsed_data = message.decode(bytes.fromhex(row["data"]))
        return parsed_data
    except Exception as e:
        print(f"Parsing error for ID {hex(row['can_id'])}: {e}")
        return None

# Read CAN log file using pandas
df = pd.read_csv(
    r"C:\Users\srama\OneDrive\Desktop\Telematics_Data\Data to Use\can_log.txt", 
    names=["timestamp", "can_id", "data"]
)

# Convert and clean data
df["timestamp"] = df["timestamp"].astype(float)
df["can_id"] = df["can_id"].apply(lambda x: int(x.split("=")[1], 16))
df["data"] = df["data"].apply(lambda x: x.split("=")[1])

# Progress tracking
total_rows = len(df)
print(f"Total rows to process: {total_rows}")

# Parse messages using pandas apply (vectorized) with progress tracking
def progress_apply(df, func):
    chunk_size = len(df) // 100  # Update progress every 1%
    results = []
    
    for i, (_, row) in enumerate(df.iterrows()):  # Unpack row from tuple
        if i % chunk_size == 0:
            print(f"Progress: {i/len(df):.2%} completed")
        results.append(func(row))
    
    return results


df["parsed_data"] = progress_apply(df, parse_can_message)

# Save parsed log to file (excluding NaN rows)
df.dropna(subset=["parsed_data"]).to_csv(PARSED_LOG_FILE, index=False, sep=",", columns=["timestamp", "can_id", "parsed_data"])

print("Parsing completed.")
