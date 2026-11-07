#!/bin/bash
# run.sh <release-image> <out-dir> acct|noacct: builds the image of
# Dockerfile.<release-image>, runs capture.sh in a container of it, and
# copies what it captured to <out-dir>. As the user who runs Docker, from
# this directory.
set -eu
image=$1 out=$2 acct=$3
name=slurm-capture-$image-$acct
docker build -q -t slurm-capture:$image -f Dockerfile.$image . >/dev/null
docker rm -f $name >/dev/null 2>&1 || true
docker run --name $name --privileged --init --hostname n1 -v "$PWD:/scripts:ro" slurm-capture:$image /scripts/capture.sh $acct
mkdir -p "$out"
docker cp $name:/out/. "$out"
docker rm -f $name >/dev/null
