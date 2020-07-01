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
| clock | string date format `yyyyMMddHHmmss` | string date format `yyyyMMddHHmmss` | `202006101558` -> `202006101558` |
| delay | string date format `yyyyMMddHHmmss` | string key:value | 202006101558 -> `my_key_1:my_message_1` |
| fired | string | string | `my_key_1` -> `my_message_1` |

Class to look in to: 
 * [DelayStream.java](src/main/java/it/stanislas/kafka/delay/streamjoin/DelayStream.java) Stream definition
 * [ClockProducer.java](src/main/java/it/stanislas/kafka/delay/streamjoin/ClockProducer.java) clock producer that emit an event every second
 * [DelayWithStreamsTest.java](src/test/java/it/stanislas/kafka/delay/DelayWithStreamsTest.java) Test on the solution

The solution has some limitation.

1. The granularity of when we can send an event is by second
2. If we fire a lot of messages at some point, like trigger millions of messages at 8:00:00 am we create an hot partition because the all delayed messages will be stored in the same partition because they have the same key. The only way to workaround this is to spread the events on different point in time.
3. We are windowing the join of a day, we can delay event only now + 1 day. We could extend the window but it could create performance issue. 

I didn't test the performance.

### Retry
TBD

## Local env

### run dependencies
```bash
docker-compose up -d
```

## TODO
* refactoring the `JSONSerde` and `JSONSerdeCompatible` implementation maybe with a type class



