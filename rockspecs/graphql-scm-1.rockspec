package = 'graphql'
version = 'scm-1'
source = {
    url = 'git://github.com/tarantool/graphql.git',
    branch = 'master',
}
description = {
    summary = 'Set of adapters for GraphQL query language to the Tarantool data model',
    homepage = 'https://github.com/tarantool/graphql',
    license = 'BSD2',
    maintainer = 'Alexander Turenko <alexander.turenko@tarantool.org>'
}
dependencies = {
    'lua >= 5.1',
    'lulpeg',
    'avro-schema',
}
build = {
    type = 'make',
    build_target = 'luarocks_build',
    install_target = 'luarocks_install',
    variables = {
        TARANTOOL_INSTALL_LUADIR = '$(LUADIR)',
    },
}
