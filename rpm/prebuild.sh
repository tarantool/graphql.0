#!/bin/sh

set -exu  # Strict shell (w/o -o pipefail)

sudo yum -y install lua-devel luarocks
sudo luarocks install ldoc
