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

```
$ docker-compose exec kafka kafka-console-consumer --bootstrap-server kafka:9092 --topic connected-component-changes --from-beginning
```

change applications:

just change the ingress path, edge type and the Kafka topic in `docker-compose.yaml` and `modules.yaml`

## issues

delete corrupted topics

```shell
docker exec hesse_kafka_1 kafka-topics --list --zookeeper zookeeper:2181
docker exec hesse_kafka_1 kafka-topics --delete --zookeeper zookeeper:2181 --topic example-temporal-graph
```