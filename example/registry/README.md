# Example
This example demonstrates using a `Registry` to create a fanout exchange and consumer-specific queues.

## Running
Start RabbitMQ locally
```
$ docker run --rm -p 5672:5672 -p 15672:15672 rabbitmq:3-management-alpine
```
Build and run
```
$ make example && ./bin/example_registry_linux
```