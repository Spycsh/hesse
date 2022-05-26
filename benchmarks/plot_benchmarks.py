import os
import matplotlib.pyplot as plt
import sys
import math
import numpy as np

# usage: python plot_benchmarks.py <folder_name>
#        python plot_benchmarks.py .

def plot_result(output_path):
    print("plot " + output_path)
    storage_time_file_path = os.path.join(output_path, "storage-time.txt")
    filter_time_file_path = os.path.join(output_path, "filter-time.txt")
    query_results_file_path = os.path.join(output_path, "query-results.txt")

    if os.path.isfile(storage_time_file_path):
        with open(storage_time_file_path, 'r') as f:
            lines = f.readlines()
            x = [int(line.split()[0]) for line in lines]
            y = [int(line.split()[1]) for line in lines]
            plt.plot(x, y)
            plt.xlabel('storage operation index')
            plt.ylabel('storage time (nanoseconds)')
            plt.title('time for each edge storage operation')
            plt.savefig(os.path.join(output_path, 'storage-time.png'), dpi=300, bbox_inches='tight')
            plt.clf()

        with open(storage_time_file_path, 'r') as f:
            lines = f.readlines()
            dx = 0.005
            x = np.array([int(line.split()[0]) for line in lines])
            data = np.array([int(line.split()[1]) for line in lines])
            sorted_data = np.sort(data)  # Or data.sort(), if data can be modified
            plt.axes(xscale="log")
            # Cumulative counts:
            plt.step(sorted_data, np.arange(sorted_data.size) / sorted_data.size)  # From 0 to the number of data points-1
            # plt.step(sorted_data[::-1], np.arange(sorted_data.size))  # From the number of data points-1 to 0
            plt.axhline(0.9, c='red', linestyle='--')
            plt.axhline(0.99, c='green', linestyle='--')
            plt.grid(which="both", ls="-", color='0.8')
            # plt.hist(data,bins=10, density=True)
            # plt.hist(data,bins=10, density=True, cumulative=True, label='CDF', histtype='step')
            # plt.plot(x, cdf)
            plt.xlabel('storage time (nanoseconds)')
            plt.ylabel('probability values')
            # plt.xticks(ticks=range(0,10), labels=["10^" + str(i) for i in range(10)])
            plt.title('CDF for continous distribution of storage time')
            plt.savefig(os.path.join(output_path, 'cdf-storage-time.png'), dpi=300, bbox_inches='tight')
            plt.clf()


    if os.path.isfile(filter_time_file_path):
        with open(filter_time_file_path, 'r') as f:
            lines = f.readlines()
            x = [int(line.split()[0]) for line in lines]
            y = [int(line.split()[1]) for line in lines]
            plt.plot(x, y)
            plt.xlabel('filter operation index')
            plt.ylabel('filter time (nanoseconds)')
            plt.title('time for each filter operation')
            plt.savefig(os.path.join(output_path, 'filter-time.png'), dpi=300, bbox_inches='tight')
            plt.clf()

        with open(filter_time_file_path, 'r') as f:
            lines = f.readlines()
            data = [math.log(float(line.split()[1]), 10) for line in lines]
            plt.hist(data,bins=10,density=True)
            # plt.hist(data,bins=10, density=True, cumulative=True, label='CDF', histtype='step')
            plt.xlabel('filter time in log scale (nanoseconds)')
            plt.ylabel('probability')
            plt.xticks(ticks=range(0,10), labels=["10^" + str(i) for i in range(10)])
            plt.title('CDF for filter time of each edge')
            plt.savefig(os.path.join(output_path, 'cdf-filter-time.png'), dpi=300, bbox_inches='tight')
            plt.clf()

    if os.path.isfile(query_results_file_path):
        with open(query_results_file_path, 'r') as f:
            lines = f.readlines()
            # query results sort
            x = sorted([int(line.split()[0]) for line in lines])
            y = sorted([int(line.split()[2]) for line in lines])
            plt.plot(x, y)
            plt.xlabel('query index')
            plt.ylabel('query time (milliseconds)')
            plt.title('time for handling each query')
            plt.savefig(os.path.join(output_path, 'query-results-time.png'), dpi=300, bbox_inches='tight')
            plt.close()

if __name__ == "__main__":
    path = sys.argv[1]
    if path == '.':
        list =  os.listdir()
        for e in list:
            if os.path.isdir(e):
                plot_result(e)
    else:
        plot_result(path)