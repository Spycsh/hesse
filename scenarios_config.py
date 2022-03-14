from typing import OrderedDict
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

QUERY_TYPES = ['hesse.types/query_mini_batch', 'hesse.types/query_strongly_connected_component']
for i in range(len(QUERY_TYPES)):
    print(str(i) + "\t" + QUERY_TYPES[i])
index_q_type = input("select the query type:\n")
query_type_name = QUERY_TYPES[int(index_q_type)]


graph_ingress_path = GRAPH_INGRESS_PATH + graph_ingress_file_name
query_ingress_path = QUERY_PATH + query_file_name
print(graph_ingress_path)
print(query_ingress_path)

with open('docker-compose.yml', 'r') as file:
    d = yaml.safe_load(file)

# spec 5 -> topics[1]
m_content = []
with open('module.yaml', 'r') as file:
    m = yaml.safe_load_all(file)
    for mm in m:
        m_content.append(mm)
        for k, v in mm.items():
            if(k == 'kind'):
                print(1)

# print(m_content)

print(d)

d['services']['hesse-producer']['environment']['APP_PATH'] = '/mnt/' + graph_ingress_path
d['services']['hesse-producer']['volumes'][0] = "./" + graph_ingress_path + ":" + '/mnt/' + graph_ingress_path

d['services']['hesse-query-producer']['environment']['APP_PATH'] = '/mnt/' + query_ingress_path
d['services']['hesse-query-producer']['volumes'][0] = "./" + query_ingress_path + ":" + '/mnt/' + query_ingress_path

idx = 4 # the index the query value type is located at
for i in range(len(m_content)):
    if i == idx:
        m_content[i]['spec']['topics'][0]['valueType'] = query_type_name

# print(m_content)

# begin to dump
with open('module.yaml', 'w') as file:
    yaml.safe_dump_all(m_content, file, sort_keys=False, default_flow_style=False)

# begin to dump
with open('docker-compose.yml', 'w') as file:
    yaml.safe_dump(d, file, sort_keys=False, default_flow_style=False)