import csv
import os
import time

# read all files from the Parsed_data folder, for each file, convert the timestamp to UTC and then to IST format 

def convert_time():
    files = os.listdir("Parsed_data")
    total_files = len(files)
    for index, file in enumerate(files):
        with open(f"Parsed_data/{file}", "r") as f:
            lines = f.readlines()

        with open(f"TimedData/time_{file}", "w", newline='') as f:
            writer = csv.writer(f)
            writer.writerow(["Timestamp", "CAN ID", "Parsed Data"])
            # skip first line of the file
            lines = lines[1:]

            for line in lines:
                timestamp, value = line.strip().split(",")
                timestamp = float(timestamp)
                timestamp = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(timestamp))
                # add 5:30 hours to timestamp to convert to ist
                timestamp = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.mktime(time.strptime(timestamp, '%Y-%m-%d %H:%M:%S')) + 19800))
                # if timestamp has date 2025-02-28 then writerow
                if timestamp[:10] == "2025-02-28":
                    writer.writerow([timestamp, value])

        print(f"Processed {index + 1}/{total_files} files.")

if __name__ == "__main__":
    convert_time()
    print("Conversion complete.")