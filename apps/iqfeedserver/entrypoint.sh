#!/usr/bin/env sh
set -e

# Find this container's host
export HOST="$(getent hosts host.docker.internal | cut -d' ' -f1)"
if [ ! $HOST ]; then
    export HOST=$(ip -4 route show default | cut -d' ' -f3)
fi

export IQFEED_HOST=$HOST

if [ "$$" = "1" ]; then
    exec tini -- "$@"
else
    exec tini -s -- "$@"
fi
