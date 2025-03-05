parse.py : Decode CAN Data using DBC files, creates a file with timestamp, parsed data
parse_multithreading.py: Same approach using ThreadPool
parse_pandas.py: Same approach with Pandas

split_data.py : Split data into individual messages
split_data_multithreading.py : Same approach using Pandas
convert_time.py : Convert time to IST from UTC

Flow:

Run parse___.py : Point the input file to the log file to be decoded

Run split_data___.py

Run convert_time.py

