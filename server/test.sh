#!/usr/bin/env bash

# set
curl -XPOST -d '{"gloss":"meaning"}' http://localhost:8080/set/test_key
curl http://localhost:8080/get/test_key