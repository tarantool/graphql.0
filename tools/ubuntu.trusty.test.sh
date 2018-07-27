#!/usr/bin/env bash

set -euxo pipefail  # Strict shell

sudo apt-get -qq update
curl http://download.tarantool.org/tarantool/1.9/gpgkey | \
    sudo apt-key add -
release=`lsb_release -c -s`

# install https download transport for APT
sudo apt-get -y install apt-transport-https

# append two lines to a list of source repositories
sudo rm -f /etc/apt/sources.list.d/*tarantool*.list
echo "deb http://download.tarantool.org/tarantool/1.9/ubuntu/ ${release} main" | \
    sudo tee /etc/apt/sources.list.d/tarantool_1_9.list
echo "deb-src http://download.tarantool.org/tarantool/1.9/ubuntu/ ${release} main" | \
    sudo tee -a /etc/apt/sources.list.d/tarantool_1_9.list

sudo apt-get update
sudo apt-get -y install tarantool tarantool-dev libmsgpuck-dev
git submodule update --recursive --init
tarantoolctl rocks install lulpeg
tarantoolctl rocks install lrexlib-pcre
tarantoolctl rocks install http
tarantoolctl rocks install shard "${SHARD_VERSION}"
tarantoolctl rocks install avro-schema "${AVRO_SCHEMA}"
sudo apt-get install luarocks
sudo luarocks install luacheck
sudo pip install virtualenv
make test
