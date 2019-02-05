#!/bin/sh

set -exu  # Strict shell (w/o -o pipefail)

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
sudo apt-get -y install tarantool tarantool-dev libmsgpuck-dev luarocks realpath

tarantoolctl rocks install lulpeg
tarantoolctl rocks install lrexlib-pcre
tarantoolctl rocks install http
tarantoolctl rocks install shard ${SHARD_VERSION:-}
tarantoolctl rocks install avro-schema ${AVRO_SCHEMA:-}

sudo luarocks install luacheck
sudo luarocks install ldoc
sudo pip install virtualenv

# luacov, cluacov, luacov-coveralls and dependencies
tarantoolctl rocks install https://raw.githubusercontent.com/keplerproject/luacov/master/luacov-scm-1.rockspec
tarantoolctl rocks install https://raw.githubusercontent.com/mpeterv/cluacov/master/cluacov-scm-1.rockspec
tarantoolctl rocks install https://raw.githubusercontent.com/LuaDist/dkjson/master/dkjson-2.5-2.rockspec
tarantoolctl rocks install https://raw.githubusercontent.com/keplerproject/luafilesystem/master/luafilesystem-scm-1.rockspec
tarantoolctl rocks install https://raw.githubusercontent.com/moteus/lua-path/master/rockspecs/lua-path-scm-0.rockspec
tarantoolctl rocks install https://raw.githubusercontent.com/moteus/luacov-coveralls/master/rockspecs/luacov-coveralls-scm-0.rockspec
