import pandas as pd
from datetime import datetime
import pytz
import time
from time import strftime, localtime
import datetime
# Dictionary to store data for each parameter
data_dict = {}

# Open and process the file
file_path = r'C:\Users\srama\OneDrive\Desktop\Telematics_Data\mqtt_parsed_log.txt'
print("Reading file...")


def convert_timestamp_to_ist(timestamp):
    # print(timestamp)
    if type(timestamp) == str:
        return timestamp
    else:
        timestamp = float(timestamp)
        timestamp = datetime.datetime.fromtimestamp(timestamp).strftime('%c')
        return timestamp



with open(file_path, 'r') as f:
    line_count = 0  # Track number of lines processed
    for line in f:
        line_count += 1
        if line_count % 1000 == 0:  # Print progress every 1000 lines
            print(f"Processed {line_count} lines...")

        time, can_id, data = line.split(',', 2)
        data = data.strip('{}')
        key_value_pairs = [kv.split(':') for kv in data.split(',')]

        for key, value in key_value_pairs:
            key = key.strip()
            value = value.strip().rstrip('}')

            if key not in data_dict:
                data_dict[key] = []  # Create a new list if key doesn't exist
            # convert linux time to ist
            time = convert_timestamp_to_ist(time)
            # time = pd.to_datetime(time, unit='s', origin='unix', utc=True)
            # time = time.tz_convert('Asia/Kolkata').strftime('%Y-%m-%d %H:%M:%S.%f')
            data_dict[key].append((time, value))  # Append data as tuple

print("Finished reading file. Writing CSV files...")

# Convert and write to CSV using pandas
for key, values in data_dict.items():
    print(f"Writing {key}.csv with {len(values)} entries...")
    df = pd.DataFrame(values, columns=['Time', 'Value'])
    df.to_csv(f"{key}.csv", index=False, mode='w', header=True)

print("All CSV files written successfully!")
