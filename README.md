# hesse

A temporal graph library based on Flink Stateful Functions

## Already Done

- [x] Architecture design and Docker environment
- [x] Kafka Graph Ingress and Query Ingress Stream
- [x] Connected Component, Strongly Connected Component, MiniBatch algorithms based on Graph Traversal
- [x] A basic non-benchmarking storage paradigm using TreeMap with persistence
- [x] Query support for three algorithms on arbitrary time window
- [x] Query cache
- [x] Time calculation for query

## TODO

- [x] User-configurable Implementation of different storage paradigms
- [x] Performance benchmarking for different storage paradigms
- [x] Measurement of time for queries of three algorithms
- [x] add Logger and set logger level to eliminate effect of print statements on time measurement
- [x] Measurement of time for ingestion of edges
- [x] Break storage TreeMap buckets into different ValueSpecs and see the performances 
- [ ] Exploration on remote and local Statefun
- [ ] Support of Single-Source-Shortest-Path algorithm and PageRank (optional)
- [ ] Query Concurrency investigation on different concurrent applications
- [ ] LRU cache of query and vertex state
- [ ] Performance benchmarking comparing with other temporal graph engines

## Features

This project aims to build a highly scalable and efficient graph processing library on top of [Flink Statefun Functions](https://nightlies.apache.org/flink/flink-statefun-docs-stable/). It provides efficient storage of temporal graph and implement the query of graph state at any specific event time for different graph algorithms.

## Architecture

The architecture is basically divided into storage, query and application layers. As you can see, they are corresponding to the three folders in the project source folder. Flink Stateful Functions guarantee that each Function serve as a service and the functions specified in this project have their own context and communicate with each other by message passing. Currently, the Kafka ingress and egress are used. The containers are built and run in Docker environment.

The basic architecture is shown as follows:

![arch old](doc/arch_hesse.png)

## How to use

There are different scenarios so far you can try out, I write a simple [script](./scenarios_config.py) to help you select the right scenario. For developers, you can write your own to replace `docker-compose.yml` and `module.yaml`.

```
python scenarios_config.py
```

build the environment and start the containers

```shell
docker-compose down
docker-compose build
docker-compose up
```

Currently, four algorithms are implemented for queries, Connected Component algorithm, Strongly Connected Component algorithm,
MiniBatch algorithm and Single Source Shortest Path algorithm.
Make sure to select the right ingress file and type using `scenarios_config.py`.

To see the results of query, you can execute the following command:

```shell
docker-compose exec kafka kafka-console-consumer --bootstrap-server kafka:9092 --topic query-results --partition 0 --from-beginning --property print.key=true --property key.separator=" ** "
```

Notice that you should set reasonable delay starting time for the query producer image using the config script based on how large your dataset is
because you may want to see correct results after your graph is fully established.

Another way is to decouple the whole `docker-compose up` into three stages: 1) edge producing, 2) edge storage 3) query producing and processing
by using the following commands:

```shell
docker-compose down
docker-compose build
# do edge producing, and you can check topic temporal-graph
# to see whether your dataset is fully pushed into topic
docker-compose up -d hesse-producer
# hesse, statefun worker and statefun manager will store
# the edges into RocksDB state backend
docker-compose up -d statefun-worker
# do query producing, and hesse will handle these queries
# and show the results in topic query-results
docker-compose up -d query-producer
```

Apart from the graph datasets and query stream that user can configure by editing the `docker-compose.yml`,
users can configure other system parameters by editing `hesse.properties` and `log4j2.properties` in the `resources` folder.

## Advanced Tips

These are still in experiments and tips for developers

* inspect the topics

```
docker-compose exec kafka kafka-console-consumer --bootstrap-server kafka:9092 --topic <replace topic name here> --from-beginning
```

Here are the exposed topics:

|topic name|usage|io form|
|---|---|---|
|temporal-graph|graph ingress stream|ingress|
|query|query stream|ingress|
|query-results|results of queries|egress|
|storage-time|time of storage used for benchmarking|egress|
|producing-time|time of producing all records by Kafka|egress|
|filter-time|time of filtering edge activities at arbitrary time windows|egress|
|indexing-time|time of indexing (only used for storage paradigm 4)|egress|

* use partition manager:

Hesse allow two ways of storage of the graph: 
1) partition by vertex id (VertexStorageFn -> Applications)
2) [Still in construction] partition by partition id (ControllerFn -> PartitionManagerFn -> Applications)

The former will create context for each vertex and rely on Flink internal partitioning scheme

The latter will offer a coarse granularity, where several nodes are classified to one partition,
and they share one context

This can also be configured in module.yaml with the ingress targets field

* delete corrupted topics

```shell
docker exec hesse_kafka_1 kafka-topics --list --zookeeper zookeeper:2181
docker exec hesse_kafka_1 kafka-topics --delete --zookeeper zookeeper:2181 --topic example-temporal-graph
```

* streaming mode

You can not only feed query records from files, but also in streaming. Just feed the query into the topic `query`,
hesse will process them and egress the results to topic `query-results`. The streaming query should for like
`<query_id>:<original_record>`. Here is an example:

```shell
docker-compose exec kafka kafka-console-producer --broker-list kafka:9092 --topic query --property parse.key=true --property key.separator=:
>5:{"query_id":"5", "user_id": "1", "vertex_id": "151", "query_type": "connected-components", "start_t": "0", "end_t": "300000"}

docker-compose exec kafka kafka-console-consumer --bootstrap-server kafka:9092 --topic query-results --partition 0 --from-beginning --property print.key=true --property key.separator=" ** "
```