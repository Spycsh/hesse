import random
import json

# e.g
# {"query_id": "1", "user_id": "1", "vertex_id": "0", "query_type": "connected-components", "start_t": "0", "end_t":"20"}

SIZE = 10           # how many queries
FILE_PATH = "synthetic_queries.txt"

#### generate the synthetic query fields

QUERY_IDS = [str(i) for i in range(SIZE)]
USER_IDS = [str(i) for i in range(SIZE)]

NODES_RANGE = 300   # nodes index range of the dataset
VERTEX_IDS = [str(i) for i in random.sample(range(NODES_RANGE), SIZE)]

QUERY_TYPES = ["connected-components" for i in range(SIZE)]

START_T_LIST = ["0" for i in range(SIZE)]
POLL = ["1000", "2000", "4000", "8000", "16000", "32000", "64000", "128000", "256000", "512000"]
END_T_LIST = random.choices(POLL, k=SIZE)

print(QUERY_IDS)
print(USER_IDS)
print(VERTEX_IDS)
print(QUERY_TYPES)
print(START_T_LIST)
print(END_T_LIST)


for i in range(SIZE):
    with open(FILE_PATH, "a+") as out_file:
        d = {}
        d["query_id"] = QUERY_IDS[i]
        d["user_id"] = USER_IDS[i]
        d["vertex_id"] = VERTEX_IDS[i]
        d["query_type"] = QUERY_TYPES[i]
        d["start_t"] = START_T_LIST[i]
        d["end_t"] = END_T_LIST[i]
        json.dump(d, out_file)
        out_file.write("\n")

