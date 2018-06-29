#!/usr/bin/env bash

set -euxo pipefail  # Strict shell

#sudo apt-get -qq update
#curl http://download.tarantool.org/tarantool/1.9/gpgkey |
#sudo apt-key add -release=`lsb_release -c -s`

# install https download transport for APT
#sudo apt-get -y install apt-transport-https

#curl -s https://packagecloud.io/install/repositories/tarantool/1_7/script.deb.sh | sudo bash
#sudo apt-get update > /dev/null
#sudo apt-get -q -y install tarantool tarantool-dev

sudo apt-get -qq update
curl http://download.tarantool.org/tarantool/1.9/gpgkey |
sudo apt-key add -release=`lsb_release -c -s`

# install https download transport for APT
sudo apt-get -y install apt-transport-https

# append two lines to a list of source repositories
sudo rm -f /etc/apt/sources.list.d/*tarantool*.list
echo "deb http://download.tarantool.org/tarantool/1.9/ubuntu/ trusty main" |
sudo tee /etc/apt/sources.list.d/tarantool_1_9.list
echo "deb-src http://download.tarantool.org/tarantool/1.9/ubuntu/ trusty main" |
sudo tee -a /etc/apt/sources.list.d/tarantool_1_9.list

sudo apt-get update > /dev/null
sudo apt-get -q -y install tarantool tarantool-dev

cd ..
git clone https://github.com/rtsisyk/msgpuck
cd msgpuck
cmake .
sudo make install
cd ..
cd graphql
git submodule update --recursive --init
tarantoolctl rocks install lulpeg
tarantoolctl rocks install lrexlib-pcre
tarantoolctl rocks install http
tarantoolctl rocks install shard "${SHARD_VERSION}"
tarantoolctl rocks install avro-schema "${AVRO_SCHEMA}"
cd ..
# lua (with dev headers) is necessary for luacheck
# maybe we can use tarantool headers?
sudo apt-get install lua5.1
sudo apt-get install liblua5.1-0-dev
wget "http://luarocks.github.io/luarocks/releases/luarocks-2.4.4.tar.gz"
tar xf luarocks-2.4.4.tar.gz
cd luarocks-2.4.4
./configure
make build
sudo make install
cd ../graphql
sudo luarocks install luacheck
sudo pip install virtualenv
make test
