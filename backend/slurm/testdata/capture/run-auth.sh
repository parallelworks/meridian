#!/bin/bash
# run-auth.sh <release-image> <out-dir>: as run.sh, with capture-auth.sh.
set -eu
image=$1 out=$2
name=slurm-capture-auth-$image
docker build -q -t slurm-capture:$image -f Dockerfile.$image . >/dev/null
docker rm -f $name >/dev/null 2>&1 || true
docker run --name $name --privileged --init --hostname n1 -v "$PWD:/scripts:ro" slurm-capture:$image /scripts/capture-auth.sh
mkdir -p "$out"
docker cp $name:/out/. "$out"
docker rm -f $name >/dev/null
