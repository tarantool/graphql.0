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
BuildRequires: python-virtualenv
BuildRequires: tarantool-luacheck
BuildRequires: tarantool >= 1.9.0.0
BuildRequires: tarantool-lulpeg
BuildRequires: tarantool-avro-schema >= 2.2.2.4
BuildRequires: tarantool-shard >= 1.1.92
BuildRequires: tarantool-lrexlib-pcre2
BuildRequires: tarantool-http

# Weak references are not supported in RHEL / CentOS.
#
# https://fedoraproject.org/wiki/Packaging:WeakDependencies
# https://bugzilla.redhat.com/show_bug.cgi?id=91458
# https://bugzilla.redhat.com/show_bug.cgi?id=1427674

# Dependencies for a user
Requires: tarantool >= 1.9.0.0
Requires: tarantool-lulpeg
Requires: tarantool-avro-schema >= 2.0.71
#Suggests: tarantool-shard >= 1.1.91
#Suggests: tarantool-lrexlib-pcre2
#Suggests: tarantool-http

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
