import sys



# INPUT_FILE = "data.csv"
OUTPUT_FILE = sys.argv[1]

log = open("python_output.log", "a")

def log_print(*args):
    print(*args)
    print(*args, file=log)
    log.flush()


data = []

log_print("Execution started")

# with open(INPUT_FILE, "r") as f:
with sys.stdin as f:
    log_print("File loaded")
    data = f.read().splitlines()

print("Data loaded")

data_arrays = [line.split(",") for line in data[1:]]
headers = data[0].split(",")[1:]

print("Data arrays initialized")

NUM_COLUMNS = len(headers)

# for line in data_arrays:
#     print(line)
print(NUM_COLUMNS)


times_to_lines = {}   # a dictionary to store each timestep and which lines of the csv file
                      # after the header, starting at 0 correlate with the timestep
 
lines_to_cols = [] # an array with which columns (after the first 3) are non-empty


for i, line in enumerate(data_arrays):
    time_stamp = int(line[2])
    if time_stamp in times_to_lines:
        times_to_lines[time_stamp].append(i)
    else:
        times_to_lines[time_stamp] = [i]
    
    lines_to_cols.append([])
    for j, datapoint in enumerate(line[3:]):
        if datapoint != "":
            lines_to_cols[-1].append(j + 3)

log_print("Helper stuff done")

# print(times_to_lines)





final_arrays = [headers]

# print(times_to_lines)

final_str = f"{','.join(headers)}\n"

for time, lines in times_to_lines.items():
    this_line = data_arrays[lines[0]][1:]
    for line in lines[1:]:
        cols = lines_to_cols[line]
        for col in cols:
            this_line[col - 1] = data_arrays[line][col] # the -1 is because the new file does not have the sensor column
    # final_arrays.append(this_line)
    final_str += f"{','.join(this_line)}\n"

log_print("Outputting to file")

# print(final_arrays)
# for arr in final_arrays:
#     final_str += f"{','.join(arr)}\n"


with open(OUTPUT_FILE, "w") as f:
    f.write(final_str)

# print(final_str)