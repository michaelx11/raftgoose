#!/bin/bash

# Curl localhost:9900-9904/goose
# and check the response
for i in {9900..9904}; do
  # Output curl response on line
  echo -n "$i: "
  curl -s localhost:$i/goose
  # Output newline
  echo ""
done
