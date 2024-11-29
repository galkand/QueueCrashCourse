
# Main steps

## Prepare 3-node kafka cluster using *KRaft*

### 1. Create these properties files:
```properties
node.id=1

process.roles=broker,controller
controller.quorum.voters=1@localhost:19093,2@localhost:29093,3@localhost:39093
controller.listener.names=CONTROLLER

listeners=PLAINTEXT://localhost:19092,CONTROLLER://localhost:19093
advertised.listeners=PLAINTEXT://localhost:19092

listener.security.protocol.map=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT

broker.id=1
log.dirs=./tmp/kafka-raft-1

num.network.threads=1
num.io.threads=1

num.partitions=12
default.replication.factor=3
offsets.topic.replication.factor=3
transaction.state.log.replication.factor=3
transaction.state.log.min.isr=2
min.insync.replicas=2
unclean.leader.election.enable=false

group.initial.rebalance.delay.ms=0

auto.create.topics.enable=true
auto.leader.rebalance.enable=true
leader.imbalance.check.interval.seconds=10
```

```properties
node.id=2

process.roles=broker,controller
controller.quorum.voters=1@localhost:19093,2@localhost:29093,3@localhost:39093
controller.listener.names=CONTROLLER

listeners=PLAINTEXT://localhost:29092,CONTROLLER://localhost:29093
advertised.listeners=PLAINTEXT://localhost:29092

listener.security.protocol.map=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT

broker.id=2
log.dirs=./tmp/kafka-raft-2

num.network.threads=1
num.io.threads=1

num.partitions=12
default.replication.factor=3
offsets.topic.replication.factor=3
transaction.state.log.replication.factor=3
transaction.state.log.min.isr=2
min.insync.replicas=2
unclean.leader.election.enable=false

group.initial.rebalance.delay.ms=0

auto.create.topics.enable=true
auto.leader.rebalance.enable=true
leader.imbalance.check.interval.seconds=10
```

```properties

node.id=3

process.roles=broker,controller
controller.quorum.voters=1@localhost:19093,2@localhost:29093,3@localhost:39093
controller.listener.names=CONTROLLER

listeners=PLAINTEXT://localhost:39092,CONTROLLER://localhost:39093
advertised.listeners=PLAINTEXT://localhost:39092

listener.security.protocol.map=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT

broker.id=3
log.dirs=./tmp/kafka-raft-3

num.network.threads=1
num.io.threads=1

num.partitions=12
default.replication.factor=3
offsets.topic.replication.factor=3
transaction.state.log.replication.factor=3
transaction.state.log.min.isr=2
min.insync.replicas=2
unclean.leader.election.enable=false

group.initial.rebalance.delay.ms=0

auto.create.topics.enable=true
auto.leader.rebalance.enable=true
leader.imbalance.check.interval.seconds=10
```
## Here is the result:

<div style="display: flex; justify-content: center; align-items: center;">
  <video width="640" height="360" controls>
    <source src="result.webm" type="video/webm">
    Your browser does not support the video tag.
  </video>
</div>
