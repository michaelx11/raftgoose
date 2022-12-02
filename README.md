# Duck Duck Goose Implementation

## Problem

Design a distributed system such that only one node is "goose" and others are "ducks", must tolerate network partitions and the "goose" should always be on the side of a quorum. Otherwise no goose.

## Solution

Chosen: Use Raft, isLeader = (True if node thinks it is leader and can execute a committed request, otherwise False)

NOTE: It is not sufficient to check the node's own state for leadership. In Raft the leader in a non-quorum partition might not ever step down (how does it know the network isn't super slow).
In our solution, we simulate a client request and wait for a committed response to return "goose".

Possible Alternative: Design a subset of Raft, but I think the probability of making some concurrency mistake in a custom protocol is too high.

**Adding Removing Nodes**

We just need to add a client request + state-machine transition for adding/removing peers one at a time. This is documented [by Eileen Pangu](https://eileen-code4fun.medium.com/raft-cluster-membership-change-protocol-f57cc17d1c03).

It's important that we can only have one add/remove peer operation in flight at any time, it must reach quorum (aka return to client as success) before doing another. Otherwise there can be a split brain situation.

**High Availability**

Using the above technique, we can add or remove hardware from the pool at any time. For pure software upgrades, we can simply stop a node and then restart it.

Note: current example implementation does not persist the database, but it's easy to imaged a version of MemoryDB that does JSON writes to a file on disk.

## References


- [original paper](https://raft.github.io/raft.pdf)
- [raft made simple](https://levelup.gitconnected.com/raft-consensus-protocol-made-simpler-922c38675181)
- [cluster size safely modifiable if one at a time](https://eileen-code4fun.medium.com/raft-cluster-membership-change-protocol-f57cc17d1c03)
- [raftscope example impl for reference](https://github.com/ongardie/raftscope/blob/5b0c10ab51f873721895e7470b49e04c94bf826f/raft.js)


## Design

Goals:
- No dependencies, so I chose pure Python + simple file storage
- Robust test framework
- Implement Raft algorithm

Core Raft logic will live in a single abstract class. Subclasses must handle the communication details.

This allows us to test the main logic without use of HTTP or other complications.

## Current State

1. Pure Python3 (no deps) Raft implementation in one file
2. Swappable database and Raft communication 
3. Simple custom test harness

Timings are not very tight (allow 0.8 seconds for leader election) but can be tuned later. Timings for MemoryRaft (the in memory version) are much tighter.

In terms of correctness, there are almost certainly subtle bugs at this point. An exhaustive test suite is required. Do not use in production.

## How to Use

If you have Python 3.7+, just run `raftgoose/http_raft.py`. The command will print out the ports addresses of the cluster nodes along with a ton of logs.

You can use scripts to interact with the system, for example choosing network partitions or pausing/killing/restarting nodes.

### Testing

Run `python raftgoose/test_basic_election.py` or `python raftgoose/test_election_partition.py` to start a 100-run version of each test.

These tests were used extensively in development, you can make them more with `--verbose`.

### Example Interaction
```
(base) ➜  duckduckgoose git:(main) ✗ bash find_goose.sh
9900: duck
9901: duck
9902: duck
9903: duck
9904: goose
(base) ➜  duckduckgoose git:(main) ✗ bash find_goose.sh
9900: duck
9901: duck
9902: duck
9903: duck
9904: goose
(base) ➜  duckduckgoose git:(main) ✗ bash startstop.sh 9904 stop
stop
(base) ➜  duckduckgoose git:(main) ✗ bash find_goose.sh
9900: duck
9901: duck
9902: duck
9903: duck
9904: duck
(base) ➜  duckduckgoose git:(main) ✗ bash find_goose.sh
9900: duck
9901: goose
9902: duck
9903: duck
9904: duck
(base) ➜  duckduckgoose git:(main) ✗ bash find_goose.sh
9900: duck
9901: goose
9902: duck
9903: duck
9904: duck
(base) ➜  duckduckgoose git:(main) ✗ bash find_goose.sh
9900: duck
9901: goose
9902: duck
9903: duck
9904: duck
(base) ➜  duckduckgoose git:(main) ✗ bash startstop.sh 9901 stop
stop
(base) ➜  duckduckgoose git:(main) ✗ bash find_goose.sh
9900: duck
9901: duck
9902: duck
9903: duck
9904: duck
(base) ➜  duckduckgoose git:(main) ✗ bash find_goose.sh
9900: goose
9901: duck
9902: duck
9903: duck
9904: duck
```

### Example Partition (hardcoded to 8900,8901 | 8902,8903,8904)

Notice how after we stop the leader in 2,3,4 partition there cannot be a new goose because no quorum
```
(base) ➜  duckduckgoose git:(main) ✗ bash find_goose.sh
9900: goose
9901: duck
9902: duck
9903: duck
9904: duck
(base) ➜  duckduckgoose git:(main) ✗ ./partition.sh
(base) ➜  duckduckgoose git:(main) ✗ bash find_goose.sh
9900: duck
9901: duck
9902: duck
9903: duck
9904: goose
(base) ➜  duckduckgoose git:(main) ✗ bash find_goose.sh
9900: duck
9901: duck
9902: duck
9903: duck
9904: goose
(base) ➜  duckduckgoose git:(main) ✗ bash startstop.sh 9904 stop
stop
(base) ➜  duckduckgoose git:(main) ✗ bash find_goose.sh
9900: duck
9901: duck
9902: duck
9903: duck
9904: duck
(base) ➜  duckduckgoose git:(main) ✗ bash find_goose.sh
9900: duck
9901: duck
9902: duck
9903: duck
```
