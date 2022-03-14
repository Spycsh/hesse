# hesse

A temporal graph library based on Flink Stateful Functions

## Features

This project aims to build a highly scalable and efficient graph processing library on top of [Flink Statefun Functions](https://nightlies.apache.org/flink/flink-statefun-docs-stable/). It provides efficient storage of temporal graph and implement the query of graph state at any specific event time for different graph algorithms.

## Architecture

The architecture is basically divided into storage, query and application layers. As you can see they are corresponding to the three folders in the project source folder. Flink Stateful Functions guarantee that each Function serve as a service and the functions specified in this project have their own context and communicate with each other by message passing. Currently, the Kafka ingress and egress are used. The containers are built and run in Docker environment.

As Flink Statefun is native to FaaS (Function as a Service), users can easily make contribution to this library and add their own independent new applications or algorithms to this library. Functions can be remote or embedded with the consistency promise by Flink internals.

The basic architecture is shown as follows:

![arch](doc/arch_hesse.png)



## How to use

There are different scenarios so far you can try out, I write a simple [script](./scenarios_config.py) to help you select the right scenario. For developers, you can write your own to replace `docker-compose.yml` and `module.yaml`.

```
python scenarios_config.py
```

build the environment and start the containers

```
docker-compose down
docker-compose build
docker-compose up
```

Currently, two algorithms are implemented for queries, Stronly Connected Component algorithm and MiniBatch algorithm. Make sure to select the right ingress file and type.

## Advanced Tips

These are still in experiments and tips for developers

* inspect the egress

```
docker-compose exec kafka kafka-console-consumer --bootstrap-server kafka:9092 --topic <replace topic name here> --from-beginning
```

* use partition manager:

Hesse allow two ways of storage of the graph: 
1) partition by vertex id (VertexStorageFn -> Applications)
2) partition by partition id (ControllerFn -> PartitionManagerFn -> Applications)

The former will create context for each vertex and rely on Flink internal partitioning scheme

The latter will offer a coarse granularity, where several nodes are classified to one partition,
and they share one context

This can also be configured in module.yaml with the ingress targets field

* delete corrupted topics

```shell
docker exec hesse_kafka_1 kafka-topics --list --zookeeper zookeeper:2181
docker exec hesse_kafka_1 kafka-topics --delete --zookeeper zookeeper:2181 --topic example-temporal-graph
```