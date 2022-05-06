import yaml
import os

"""
This file is a simple script for configuration of different testing scenarios
It is not necessary and you can manually replace the configuration in docker-compose.yml and modules.yml
"""

GRAPH_INGRESS_PATH = 'datasets/graph/'
QUERY_PATH = 'datasets/query/'

names = os.listdir(GRAPH_INGRESS_PATH)
d_g = {str(i):names[i] for i in range(len(names))}
for key in d_g.keys():
    print(key + "\t" + d_g[key])
index_g = input("select the graph edges ingress (enter the index) :\n")
graph_ingress_file_name = d_g[index_g]

print("\n")

names = os.listdir(QUERY_PATH)
d_q = {str(i):names[i] for i in range(len(names))}
for key in d_q.keys():
    print(key + "\t" + d_q[key])
index_q = input("select the query ingress (enter the index) :\n")
query_file_name = d_q[index_q]

print("\n")

query_producer_delay_time = input("how long in seconds would you delay the start of query producer:\n")

print("\n")

graph_ingress_path = GRAPH_INGRESS_PATH + graph_ingress_file_name
query_ingress_path = QUERY_PATH + query_file_name
print(graph_ingress_path)
print(query_ingress_path)

with open('docker-compose.yml', 'r') as file:
    d = yaml.safe_load(file)

m_content = []
with open('module.yaml', 'r') as file:
    m = yaml.safe_load_all(file)
    for mm in m:
        m_content.append(mm)

d['services']['hesse-producer']['environment']['APP_PATH'] = '/mnt/' + graph_ingress_path
d['services']['hesse-producer']['volumes'][0] = "./" + graph_ingress_path + ":" + '/mnt/' + graph_ingress_path

d['services']['hesse-query-producer']['environment']['APP_PATH'] = '/mnt/' + query_ingress_path
d['services']['hesse-query-producer']['volumes'][0] = "./" + query_ingress_path + ":" + '/mnt/' + query_ingress_path
d['services']['hesse-query-producer']['environment']['APP_DELAY_START_SECONDS'] = query_producer_delay_time

with open('docker-compose.yml', 'w') as file:
    yaml.safe_dump(d, file, sort_keys=False, default_flow_style=False)

print("Success initialize")