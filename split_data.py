import csv

# open the file
with open(r'C:\Users\srama\OneDrive\Desktop\Telematics_Data\mqtt_parsed_log.txt', 'r') as f:
    for line in f:
        time, can_id, data = line.split(',', 2)
        # verify what data is in curly braces,
        data = data.strip('{}')
        data = data.split(',')

        # extract all values inside data and store them in their respective csv files
        for i in data:
            i = i.split(':')
            i[0] = i[0].strip()
            i[1] = i[1].strip().rstrip('}')
            # check if the file exists
            try:
                with open(i[0] + '.csv', 'a', newline='') as file:
                    writer = csv.writer(file)
                    writer.writerow([time, i[1]])
            except:
                with open(i[0] + '.csv', 'w', newline='') as file:
                    writer = csv.writer(file)
                    writer.writerow(['Time', 'Value'])
                    writer.writerow([time, i[1]])
