# kafka-delay-retry

## Problem to solve
In this project I would like to solve two problems:
1) do an action after x time or at point in time triggered from a kafka event
2) retry an action, triggered from a kafka event, n time with with different delay on each retry.

To do that I would like to explore if kafka stream could help me to solve those problems.

## Problem examples

### Delay
TBD

### Retry
TBD

## Local env

### run dependencies
```bash
docker-compose up -d
```

## TODO
* refactoring the `JSONSerde` and `JSONSerdeCompatible` implementation maybe with a type class



