# Duck Duck Goose Implementation


## Solution

Chosen: Use Raft, isLeader = (True if node thinks it is leader and can execute a committed request, otherwise False)

Possible: Design a subset of Raft, but I think the probability of making some concurrency mistake in a custom protocol is too high.


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

## How to Use

If you have Python 3.7+, just run `start_cluster.py [num nodes]`. The command will print out the IP addresses of the cluster nodes.

You can use the console to interact with the system, for example choosing network partitions or pausing/killing/restarting nodes.
