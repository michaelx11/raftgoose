#!/bin/bash

set -e

# Sends 5 curl requests to ports 9900 - 9904
# with json payload of: {"peers": ["8900", ...]} where peers contains nodes to drop messages to and from

# Partition [0,1] from [2,3,4] (messy I know it's late and gets the job done)
curl -X POST -H "Content-Type: application/json" -d '{"peers": ["8902", "8903", "8904"]}' http://localhost:9900/partition
if [ $? -ne 0 ]; then
    echo "Failed to partition"
    exit 1
fi
curl -X POST -H "Content-Type: application/json" -d '{"peers": ["8902", "8903", "8904"]}' http://localhost:9901/partition
if [ $? -ne 0 ]; then
    echo "Failed to partition"
    exit 1
fi
curl -X POST -H "Content-Type: application/json" -d '{"peers": ["8900", "8901"]}' http://localhost:9902/partition
if [ $? -ne 0 ]; then
    echo "Failed to partition"
    exit 1
fi
curl -X POST -H "Content-Type: application/json" -d '{"peers": ["8900", "8901"]}' http://localhost:9903/partition
if [ $? -ne 0 ]; then
    echo "Failed to partition"
    exit 1
fi
curl -X POST -H "Content-Type: application/json" -d '{"peers": ["8900", "8901"]}' http://localhost:9904/partition
if [ $? -ne 0 ]; then
    echo "Failed to partition"
    exit 1
fi
