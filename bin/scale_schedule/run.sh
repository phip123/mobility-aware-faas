#!/usr/bin/env bash

set -o allexport
# shellcheck disable=SC1090
source ./bin/scale_schedule/$1.env
set +o allexport

python -m schedulescaleopt.cli.$1