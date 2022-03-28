#!/bin/bash

ROOT_DIR=`realpath $(dirname $0)/..`

# Use BuildKit as docker builder
export DOCKER_BUILDKIT=1

function build_boki {
    docker build -t zjia/boki:sosp-ae \
        -f $ROOT_DIR/dockerfiles/Dockerfile.boki \
        $ROOT_DIR/boki
}

function build_queuebench {
    docker build -t zjia/boki-queuebench:sosp-ae \
        -f $ROOT_DIR/dockerfiles/Dockerfile.queuebench \
        $ROOT_DIR/workloads/queue
}

function build_retwisbench {
    docker build -t joshuaweintraub/boki-retwisbench:sosp-ae \
        -f $ROOT_DIR/dockerfiles/Dockerfile.retwisbench \
        $ROOT_DIR/workloads/retwis
}

function build_beldibench {
    docker build -t zjia/boki-beldibench:sosp-ae \
        -f $ROOT_DIR/dockerfiles/Dockerfile.beldibench \
        $ROOT_DIR/workloads/workflow
}

function build {
    # build_boki
    # build_queuebench
    build_retwisbench
    # build_beldibench
}

function push {
    # docker push zjia/boki:sosp-ae
    # docker push zjia/boki-queuebench:sosp-ae
    docker push joshuaweintraub/boki-retwisbench:sosp-ae
    # docker push zjia/boki-beldibench:sosp-ae
}

case "$1" in
build)
    build
    ;;
push)
    push
    ;;
esac
