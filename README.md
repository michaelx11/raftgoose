# Duck Duck Goose Implementation



## References

[raft made simple](https://levelup.gitconnected.com/raft-consensus-protocol-made-simpler-922c38675181)
[cluster size safely modifiable if one at a time](https://eileen-code4fun.medium.com/raft-cluster-membership-change-protocol-f57cc17d1c03)

## Design

Goals:
- No dependencies, so I chose pure Python + simple file storage
- Robust test framework
- Implement Raft algorithm

Core Raft logic will live in a single abstract class. Subclasses must handle the communication details.

This allows us to test the main logic without use of HTTP or other complications.
