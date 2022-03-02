# hesse
a temporal graph library based on Flink Stateful Functions

## How to use

build the environment

```
$ docker-compose build
```

start the containers

```
$ docker-compose up
```

inspect the egress

Currently, we build two scenarios:

1) Find Connected Components (unweighted graph)
```
$ docker-compose exec kafka kafka-console-consumer --bootstrap-server kafka:9092 --topic connected-component-changes --from-beginning
```

2) Find single source shortest path (weighted graph)
```
$ docker-compose exec kafka kafka-console-consumer --bootstrap-server kafka:9092 --topic single-source-shortest-path-
changes --from-beginning
```

change applications:

just change the ingress file (or stream) path, edge type (weighted or unweighted) and the Kafka topic in `docker-compose.yaml` and `modules.yaml`
The comments in those two yaml files will help you with the config

use partition manager:

Hesse allow two ways of storage of the graph: 
1) partition by vertex id (VertexStorageFn -> Applications)
2) partition by partition id (ControllerFn -> PartitionManagerFn -> Applications)

The former will create context for each vertex and rely on Flink internal partitioning scheme

The latter will offer a coarse granularity, where several nodes are classified to one partition,
and they share one context

This can also be configured in module.yaml with the ingress targets field

## issues

delete corrupted topics

```shell
docker exec hesse_kafka_1 kafka-topics --list --zookeeper zookeeper:2181
docker exec hesse_kafka_1 kafka-topics --delete --zookeeper zookeeper:2181 --topic example-temporal-graph
```

## query

[comment]: <> (```shell)

[comment]: <> (curl -X PUT -H "Content-Type: application/vnd.hesse.types/query_mini_batch" -d {"query_id": "1", "user_id": "1", "vertex_id": "1", "query_type": "mini-batch", "T": "50", "H": "2", "K": 2 } localhost:8091/hesse.query/temporal-query-handler/1)

[comment]: <> (```)