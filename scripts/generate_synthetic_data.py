
import random
import json

RANGE_START = 0
RANGE_END = 530000
SIZE = 100000
FILE_PATH = "synthetic_benchmarks.txt"
UNDIRECTED_FLAG = True

def generate_synthetic_data(r=(0, 70000000), size=100000, allow_duplicated=False):
    if allow_duplicated:
        return [str(random.randint(r[0], r[1])) for i in range(size)]
    else:
        samples = random.sample(range(r[0], r[1]), size)
        return [str(i) for i in samples]
    

timestamp_list = generate_synthetic_data((RANGE_START, RANGE_END), SIZE, False)
src_id = ["151" for i in range(SIZE)]
dst_id = generate_synthetic_data((0, 1000), SIZE, True)

print(timestamp_list)
print(src_id)
print(dst_id)

for i in range(len(timestamp_list)):
    # if not exist the file, create it in add mode
    with open(FILE_PATH, "a+") as out_file:
        d = {}
        d["src_id"] = src_id[i]
        d["dst_id"] = dst_id[i]
        d["timestamp"] = timestamp_list[i]
        json.dump(d, out_file)
        out_file.write("\n")
        if UNDIRECTED_FLAG:
            d_reverse = {}
            d_reverse["src_id"] = dst_id[i]
            d_reverse["dst_id"] = src_id[i]
            d_reverse["timestamp"] = timestamp_list[i]
            json.dump(d_reverse, out_file)
            out_file.write("\n")