import os
import matplotlib.pyplot as plt
import sys

def plot_result(output_path):
    print("plot " + output_path)
    storage_time_file_path = os.path.join(output_path, "storage-time.txt")
    filter_time_file_path = os.path.join(output_path, "filter-time.txt")
    query_results_file_path = os.path.join(output_path, "query-results.txt")

    if os.path.isfile(storage_time_file_path):
        with open(storage_time_file_path, 'r') as f:
            lines = f.readlines()
            x = [int(line.split()[0]) for line in lines]
            y = [int(float(line.split()[3])) for line in lines]
            plt.axes(yscale="log")
            plt.plot(x, y)
            plt.xlabel('storage operation index')
            plt.ylabel('average storage time (nanoseconds)')
            plt.title('time for each edge storage operation')
            plt.savefig(os.path.join(output_path, 'average-storage-time.png'), dpi=300, bbox_inches='tight')
            plt.clf()

    if os.path.isfile(filter_time_file_path):
        with open(filter_time_file_path, 'r') as f:
            lines = f.readlines()
            x = [int(line.split()[0]) for line in lines]
            y = [int(float(line.split()[3])) for line in lines]
            plt.axes(yscale="log")
            plt.plot(x, y)
            plt.xlabel('filter operation index')
            plt.ylabel('average filter time (nanoseconds)')
            plt.title('time for each filter operation')
            plt.savefig(os.path.join(output_path, 'average-filter-time.png'), dpi=300, bbox_inches='tight')
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