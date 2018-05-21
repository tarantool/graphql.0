#!/bin/sh

set -exu  # Strict shell (w/o -o pipefail)

sudo yum -y install epel-release

# XXX: How to enable 1_9, 1_10 or 2_0 conditionally depending on target
# repository in CI / CD?

sudo tee /etc/yum.repos.d/tarantool_1_9.repo <<- EOF
[tarantool_1_9]
name=EnterpriseLinux-7 - Tarantool
baseurl=http://download.tarantool.org/tarantool/1.9/el/7/x86_64/
gpgkey=http://download.tarantool.org/tarantool/1.9/gpgkey
repo_gpgcheck=1
gpgcheck=0
enabled=1

[tarantool_1_9-source]
name=EnterpriseLinux-7 - Tarantool Sources
baseurl=http://download.tarantool.org/tarantool/1.9/el/7/SRPMS
gpgkey=http://download.tarantool.org/tarantool/1.9/gpgkey
repo_gpgcheck=1
gpgcheck=0
EOF

sudo yum makecache -y --disablerepo='*' --enablerepo='tarantool_1_9' --enablerepo='epel'

# XXX: It would be good to have these packages on our RPM repository and
# install it as BuildRequires.

sudo yum -y install luarocks lua-devel
sudo luarocks install luacheck

sudo yum -y install tarantool tarantool-devel \
    msgpuck-devel \
    pcre2 pcre2-devel
cd / && sudo tarantoolctl rocks install lulpeg
cd / && sudo tarantoolctl rocks install lrexlib-pcre2
cd / && sudo tarantoolctl rocks install avro-schema 2.3.2
cd / && sudo tarantoolctl rocks install shard 2.1
cd / && sudo tarantoolctl rocks install http
