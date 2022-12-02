#!/bin/bash

set -e

# Print usage: ./stopstart.sh [port] [start|stop]
if [ $# -ne 2 ]; then
    echo "Usage: $0 [port] [start|stop]"
    exit 1
fi

# Make a curl request to /action at http://localhost:$1
# $1 is the port number
# $2 is the action (start or stop)
curl -s -X POST http://localhost:$1/$2 -d '{}'

# If the curl request was successful, echo "action"
if [ $? -eq 0 ]; then
    echo $2
else
    echo "Error: $2 failed"
fi
