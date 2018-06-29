Name: tarantool-graphql
# During package building {version} is overwritten by Packpack with
# VERSION. It is set to major.minor.patch.number_of_commits_above_last_tag.
# major.minor.patch tag and number of commits above are taken from the
# github repository: https://github.com/tarantool/graphql
Version: 0.0.1
Release: 1%{?dist}
Summary: Set of adapters for GraphQL query language to the Tarantool data model
Group: Applications/Databases
License: BSD
URL: https://github.com/tarantool/graphql
Source0: https://github.com/tarantool/graphql/archive/%{version}/graphql-%{version}.tar.gz
BuildArch: noarch

# Dependencies for `make test`
BuildRequires: tarantool >= 1.9.0.0
BuildRequires: tarantool-avro-schema >= 2.2.2.4
BuildRequires: tarantool-shard >= 2.1.0
BuildRequires: tarantool-http
BuildRequires: python-virtualenv
BuildRequires: tarantool-luacheck
BuildRequires: tarantool-lulpeg
BuildRequires: tarantool-lrexlib-pcre2

# Dependencies for a user
Requires: tarantool >= 1.9.0.0
Requires: tarantool-avro-schema >= 2.0.0
Requires: tarantool-lulpeg
#Suggests: tarantool-lrexlib-pcre2
#Suggests: tarantool-shard
#Suggests: tarantool-http
#Suggests: tarantool-avro-schema

%description
Set of adapters for GraphQL query language to the Tarantool data model

# /usr/share/tarantool
%define module_dir    %{_datadir}/tarantool
%define br_module_dir %{buildroot}%{module_dir}

%prep
%setup -q -n %{name}-%{version}

%check
# Originally 'check' section is executed in
# /build/usr/src/degub/tarantool-graphql directory.
# It makes names of unix sockets too long and therefore tests fail.
# To avoid it we copy sources to /build/graphql and run tests there.
cp -R . /build/graphql
cd /build/graphql
make test

%install
mkdir -p %{br_module_dir}
cp -r graphql %{br_module_dir}

%files
%{module_dir}/graphql

%changelog
* Thu Jul 12 2018 Ivan Koptelov <ivan.koptelov@tarantool.org> 0.0.1-1
- Initial release 0.0.1

* Sun May 20 2018 Alexander Turenko <alexander.turenko@tarantool.org> 0.0.0-1
- Create pseudo-release 0.0.0 for testing deployment
