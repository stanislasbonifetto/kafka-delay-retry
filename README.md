# kafka-delay-retry

## Problem to solve
In this project I would like to solve two problems:
1) do an action after x time or at point in time triggered from a kafka event
2) retry an action, triggered from a kafka event, n time with with different delay on each retry.

To do that I would like to explore if kafka stream could help me to solve those problems.

## Problem examples

### Delay
The problem we try to solve is to delay a message and trigger at a point in time.

#### Solution with a kafka stream join
This solution is using kafka stream and the join operation.
The idea is to have 3 topics:
1. `clock` -> where we are going to stream messages each seconds
2. `delay` -> where we are going to stream the messages that we want delay
3. `fired` -> where the messages streamed in the `delay` topic will be fired at the right point in time

| topic | key | value | key - > value example |
| --- | --- | --- | --- |
| clock | string date formart `yyyyMMddHHmmss` | string date formart `yyyyMMddHHmmss` | `202006101558` -> `202006101558` |
| delay | string date formart `yyyyMMddHHmmss` | string key:value | 202006101558 -> `my_key_1:my_message_1` |
| fired | string | string | `my_key_1` -> `my_message_1` |

### Retry
TBD

## Local env

### run dependencies
```bash
docker-compose up -d
```

## TODO
* refactoring the `JSONSerde` and `JSONSerdeCompatible` implementation maybe with a type class



