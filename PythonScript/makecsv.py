import sys

log = open("python_output.log", "a")

def log_print(*args):
    print(*args)
    print(*args, file=log)
    log.flush()

for line in sys.stdin:
    line = line.strip()
    if line.startswith("HEADERS:"):
        log_print("Got headers:", line[len("HEADERS:"):])
    elif line.startswith("ROW:"):
        log_print("Got row:", line[len("ROW:"):])
