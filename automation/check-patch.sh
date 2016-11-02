#!/bin/bash

EXPORT_DIR="$PWD/exported-artifacts"

set -xe

# This allows us to check whether we are running under Jenkins or not.
export VDSM_AUTOMATION=1

easy_install pip
pip install -U tox==2.1.1

./autogen.sh --system --enable-hooks --enable-vhostmd

debuginfo-install -y python

TIMEOUT=600 make check NOSE_WITH_COVERAGE=1 NOSE_COVER_PACKAGE="$PWD/vdsm,$PWD/lib"

# Generate coverage report in HTML format
pushd tests
coverage html -d "$EXPORT_DIR/htmlcov"
popd

# enable complex globs
shopt -s extglob
# In case of vdsm specfile or any Makefile.am file modification in commit,
# try to build and install all new created packages
if git diff-tree --no-commit-id --name-only -r HEAD | egrep --quiet 'vdsm.spec.in|Makefile.am' ; then
    ./automation/build-artifacts.sh
    yum -y install "$EXPORT_DIR/"!(*.src).rpm
fi
