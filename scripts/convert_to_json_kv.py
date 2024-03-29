file_path = input("path to the graph file you want to change to json format: ") # email-Eu-core-temporal-Dept1.txt
new_file_path = input("path to the new file you want your json to be written to: ") # email_EU_edges_undirected.txt
undirected_flag = input("whether you want to add two-way edge for undirected graph, y/n? ")
line_num = int(input("how many lines to generate: "))

import json

with open(file_path) as file:
    lines = file.readlines()
    for line in lines:
        if line_num == 0:
            break
        arr = line.split(" ")
        d = {}
        d["src_id"] = arr[0]
        d["dst_id"] = arr[1]
        d["timestamp"] = arr[2].replace('\n', '')

        d_reverse = {}
        if(undirected_flag in ["y", "yes", "Y", "YES"]):
            d_reverse["src_id"] = arr[1]
            d_reverse["dst_id"] = arr[0]
            d_reverse["timestamp"] = arr[2].replace('\n', '')

        # if not exist the file, create it in add mode
        with open(new_file_path, "a+") as out_file:
            out_file.write("{}: ".format(d["src_id"]))
            json.dump(d, out_file)
            out_file.write("\n")
            line_num -= 1
            if(undirected_flag in ["y", "yes", "Y", "YES"]):
                line_num -= 1
                out_file.write("{}: ".format(d_reverse["src_id"]))
                json.dump(d_reverse, out_file)
                out_file.write("\n")