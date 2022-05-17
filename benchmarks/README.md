This folder contains all the benchmarking results.

For one execution of Hesse, one folder will be created that contains five text files.
Those five text files are `filter-time.txt`, `indexing-time.txt`,
`query-results.txt` and `storage-time.txt`. They represent different benchmarking results
under different metrics. They subscribe to the exposed egress topics of Hesse and store persistent
data for further analysis.

In details, data of the five files are formed like following:

* query-results

```
# time is in milliseconds
qid uid time result_string
```

* filter-time & storage-time

```
# in nanoseconds
record_number time overall_time average_time
```

* indexing-time  
a number in nanoseconds

