Name: tarantool-graphql
Version: 0.0.0
Release: 1%{?dist}
Summary: Set of adapters for GraphQL query language to the Tarantool data model
Group: Applications/Databases
License: BSD
URL: https://github.com/tarantool/graphql
Source0: https://github.com/tarantool/graphql/archive/%{version}/graphql-%{version}.tar.gz
BuildArch: noarch

# Dependencies for `make test`
BuildRequires: tarantool >= 1.9.0.0
#BuildRequires: tarantool-avro-schema >= 2.2.2.4, tarantool-avro-schema < 3.0.0.0
#BuildRequires: tarantool-shard >= 2.1.0
BuildRequires: python-virtualenv
# Dependencies were installed in rpm/prebuild-el-7.sh:
# * luacheck
# * lulpeg
# * lrexlib-pcre2
# * avro-schema
# * shard
# * http

# Dependencies for a user
Requires: tarantool >= 1.9.0.0
#Requires: tarantool-avro-schema >= 2.0.71, tarantool-avro-schema < 3.0.0.0
#Requires: lulpeg

%description
Set of adapters for GraphQL query language to the Tarantool data model

# /usr/share/tarantool
%define module_dir    %{_datadir}/tarantool
%define br_module_dir %{buildroot}%{module_dir}

%prep
%setup -q -n %{name}-%{version}

%check
make test

%install
mkdir -p %{br_module_dir}
cp -r graphql %{br_module_dir}

%files
%{module_dir}/graphql

%changelog

* Sun May 20 2018 Alexander Turenko <alexander.turenko@tarantool.org>
- create pseudo-release 0.0.0 for testing deployment
