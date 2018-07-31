#!/bin/sh

set -exu  # Strict shell (w/o -o pipefail)

sudo yum -y install https://centos6.iuscommunity.org/ius-release.rpm
sudo yum -y install python27 python27-devel
