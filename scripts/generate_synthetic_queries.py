import random
import json

# for micro-benchmarking
# e.g
# {"query_id": "1", "user_id": "1", "vertex_id": "0", "query_type": "connected-components", "start_t": "0", "end_t":"20"}

SIZE = 100           # how many queries
VERTICES_SIZE = 20 # how many vertices
FILE_PATH = "synthetic_queries.txt"

#### generate the synthetic query fields

QUERY_IDS = [str(i) for i in range(1, SIZE+1)]
USER_IDS = [str(i) for i in range(1, SIZE+1)]

NODES_RANGE = 300   # nodes index range of the dataset
VERTICES_IDS_ = [str(i) for i in random.sample(range(NODES_RANGE), VERTICES_SIZE)]
VERTICES_IDS = []
for i in range(5):
    VERTICES_IDS += VERTICES_IDS_

QUERY_TYPES = ["connected-components" for i in range(SIZE)]

START_T_LIST = ["0" for i in range(SIZE)]
POLL = ["100", "1000", "10000", "100000", "1000000"]
END_T_LIST = []
for i in range(5):
    for j in range(VERTICES_SIZE):
        END_T_LIST = END_T_LIST + [POLL[i]]

print(QUERY_IDS)
print(USER_IDS)
print(VERTICES_IDS)
print(QUERY_TYPES)
print(START_T_LIST)
print(END_T_LIST)


for i in range(SIZE):
    with open(FILE_PATH, "a+") as out_file:
        d = {}
        d["query_id"] = QUERY_IDS[i]
        d["user_id"] = USER_IDS[i]
        d["vertex_id"] = VERTICES_IDS[i]
        d["query_type"] = QUERY_TYPES[i]
        d["start_t"] = START_T_LIST[i]
        d["end_t"] = END_T_LIST[i]
        json.dump(d, out_file)
        out_file.write("\n")

