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
$ docker-compose exec kafka kafka-console-consumer --bootstrap-server kafka:9092 --topic partition-edges --from-beginning
```