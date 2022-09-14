#!/usr/bin/env bash

# script to build x86 docker images for local usage.
# we're assuming that you are using a an x86 machine.

BASE="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT=$(realpath "${BASE}/../")

name=osmotic-loadbalancer-optimizer
image=edgebench/$name

docker build -t $image:"$(git rev-parse --short HEAD)" -f docker/Dockerfile.amd64 .
