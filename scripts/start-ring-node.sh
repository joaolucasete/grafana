#!/bin/bash
set -eo pipefail

ADDR=$1
MEMBERS="${@:2}"

GF_SERVER_HTTP_ADDR=$ADDR GF_GRPC_SERVER_ADDRESS="$ADDR:5000" GF_REMOTE_CACHE_RING_JOIN_MEMBERS=$MEMBERS  make run
