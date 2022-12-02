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

If you have Python 3.7+, just run `raftgoose/http_raft.py`. The command will print out the ports addresses of the cluster nodes along with a ton of logs.

You can use scripts to interact with the system, for example choosing network partitions or pausing/killing/restarting nodes.

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
