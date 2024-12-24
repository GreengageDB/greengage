#!/bin/bash

set -ex

# clone the repos
rm -fr gpos
rm -fr gporca

pushd ~
  git clone https://github.com/GreengageDB/gpos
  git clone https://github.com/GreengageDB/gporca
popd

export CC="ccache cc"
export CXX="ccache c++"
export PATH=/usr/local/bin:$PATH

pushd ~/gpos
  rm -fr build
  mkdir build
  pushd build
    cmake ../
    make -j4 && make install
  popd
popd

pushd ~/gporca
  rm -fr build
  mkdir build
  pushd build
    cmake ../
    make -j4 && make install
  popd
popd
