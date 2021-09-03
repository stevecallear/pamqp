# Example
This example demonstrates using statically created infrastructure to configure a topic exchange and associated queue.

## Running
Start RabbitMQ locally
```
$ docker run --rm -p 5672:5672 -p 15672:15672 rabbitmq:3-management-alpine
```
Build and run
```
$ make example && ./bin/example_static_linux
```