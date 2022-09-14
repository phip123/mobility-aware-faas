#!/usr/bin/env bash

set -o allexport
# shellcheck disable=SC1090
source ./bin/lbopt/.env
set +o allexport

python -m lbopt.cli.$1