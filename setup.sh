#!/bin/bash
# Install prereqs
sudo apt update
sudo apt install -y cmake build-essential autoconf libtool pkg-config

# Setup repo
git submodule update --init --recursive

GRPC_INSTALL_DIR=$(pwd)/grpc_install
mkdir -p $GRPC_INSTALL_DIR

# Build gRPC
pushd grpc
mkdir -p cmake/build
cd cmake/build
cmake -DgRPC_INSTALL=ON \
	-DgRPC_BUILD_TESTS=OFF \
	-DCMAKE_INSTALL_PREFIX=$GRPC_INSTALL_DIR \
	../..
make -j8
make install
popd

# Setup cmake
PATH=$GRPC_INSTALL_DIR/bin:$PATH
mkdir -p cmake/build
pushd cmake/build
cmake ../..
popd
