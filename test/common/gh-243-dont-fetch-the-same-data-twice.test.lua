#!/usr/bin/env tarantool

-- https://github.com/tarantool/graphql/issues/243

local fio = require('fio')

-- require in-repo version of graphql/ sources despite current working directory
package.path = fio.abspath(debug.getinfo(1).source:match("@?(.*/)")
    :gsub('/./', '/'):gsub('/+$', '')) .. '/../../?.lua' .. ';' .. package.path

local tap = require('tap')
local test_utils = require('test.test_utils')

local testdata = {
    meta = {
        schemas = {
            -- for case: same keys in a batch
            a = {
                type = 'record',
                name = 'a',
                fields = {
                    {name = 'id', type = 'string'},
                    {name = 'b_id', type = 'string'},
                },
            },
            b = {
                type = 'record',
                name = 'b',
                fields = {
                    {name = 'id', type = 'string'},
                },
            },
            -- for case: partially overlaping batches
            w = {
                type = 'record',
                name = 'w',
                fields = {
                    {name = 'id', type = 'string'},
                },
            },
            x = {
                type = 'record',
                name = 'x',
                fields = {
                    {name = 'id', type = 'string'},
                    {name = 'w_id', type = 'string'},
                    {name = 'z_id', type = 'string'},
                },
            },
            y = {
                type = 'record',
                name = 'y',
                fields = {
                    {name = 'id', type = 'string'},
                    {name = 'w_id', type = 'string'},
                    {name = 'z_id', type = 'string'},
                },
            },
            z = {
                type = 'record',
                name = 'z',
                fields = {
                    {name = 'id', type = 'string'},
                },
            },
        },
        collections = {
            -- a --1:1--> b
            a = {
                schema_name = 'a',
                connections = {
                    {
                        type = '1:1',
                        name = 'b',
                        destination_collection = 'b',
                        parts = {
                            {source_field = 'b_id', destination_field = 'id'},
                        },
                        index_name = 'id',
                    },
                },
            },
            b = {
                schema_name = 'b',
                connections = {},
            },
            -- w --1:N--> x
            -- w --1:N--> y
            w = {
                schema_name = 'w',
                connections = {
                    {
                        type = '1:N',
                        name = 'x',
                        destination_collection = 'x',
                        parts = {
                            {source_field = 'id', destination_field = 'w_id'},
                        },
                        index_name = 'w_id',
                    },
                    {
                        type = '1:N',
                        name = 'y',
                        destination_collection = 'y',
                        parts = {
                            {source_field = 'id', destination_field = 'w_id'},
                        },
                        index_name = 'w_id',
                    },
                },
            },
            -- x --1:1--> z
            x = {
                schema_name = 'x',
                connections = {
                    {
                        type = '1:1',
                        name = 'xz',
                        destination_collection = 'z',
                        parts = {
                            {source_field = 'z_id', destination_field = 'id'},
                        },
                        index_name = 'id',
                    },
                },
            },
            -- y --1:1--> z
            y = {
                schema_name = 'y',
                connections = {
                    {
                        type = '1:1',
                        name = 'yz',
                        destination_collection = 'z',
                        parts = {
                            {source_field = 'z_id', destination_field = 'id'},
                        },
                        index_name = 'id',
                    },
                },
            },
            z = {
                schema_name = 'z',
                connections = {},
            },
        },
        service_fields = {
            a = {},
            b = {},
            w = {},
            x = {},
            y = {},
            z = {},
        },
        indexes = {
            a = {
                id = {
                    service_fields = {},
                    fields = {'id'},
                    index_type = 'tree',
                    unique = true,
                    primary = true,
                },
            },
            b = {
                id = {
                    service_fields = {},
                    fields = {'id'},
                    index_type = 'tree',
                    unique = true,
                    primary = true,
                },
            },
            w = {
                id = {
                    service_fields = {},
                    fields = {'id'},
                    index_type = 'tree',
                    unique = true,
                    primary = true,
                },
            },
            x = {
                id = {
                    service_fields = {},
                    fields = {'id'},
                    index_type = 'tree',
                    unique = true,
                    primary = true,
                },
                w_id = {
                    service_fields = {},
                    fields = {'w_id'},
                    index_type = 'tree',
                    unique = false,
                    primary = false,
                },
            },
            y = {
                id = {
                    service_fields = {},
                    fields = {'id'},
                    index_type = 'tree',
                    unique = true,
                    primary = true,
                },
                w_id = {
                    service_fields = {},
                    fields = {'w_id'},
                    index_type = 'tree',
                    unique = false,
                    primary = false,
                },
            },
            z = {
                id = {
                    service_fields = {},
                    fields = {'id'},
                    index_type = 'tree',
                    unique = true,
                    primary = true,
                },
            },
        },
    },

    init_spaces = function()
        local A_ID_FN = 1
        local B_ID_FN = 1
        local W_ID_FN = 1
        local X_ID_FN = 1
        local X_W_ID_FN = 2
        local Y_ID_FN = 1
        local Y_W_ID_FN = 2
        local Z_ID_FN = 1

        box.once('gh-243-init-spaces', function()
            -- create 'a' and 'a.id'
            box.schema.create_space('a')
            box.space.a:create_index('id',
                {type = 'tree', unique = true, parts = {
                    A_ID_FN, 'string',
                }}
            )

            -- create 'b' and 'b.id'
            box.schema.create_space('b')
            box.space.b:create_index('id',
                {type = 'tree', unique = true, parts = {
                    B_ID_FN, 'string',
                }}
            )

            -- create 'w' and 'w.id'
            box.schema.create_space('w')
            box.space.w:create_index('id',
                {type = 'tree', unique = true, parts = {
                    W_ID_FN, 'string',
                }}
            )

            -- create 'x', 'x.id', 'x.w_id'
            box.schema.create_space('x')
            box.space.x:create_index('id',
                {type = 'tree', unique = true, parts = {
                    X_ID_FN, 'string',
                }}
            )
            box.space.x:create_index('w_id',
                {type = 'tree', unique = false, parts = {
                    X_W_ID_FN, 'string',
                }}
            )

            -- create 'y', 'y.id', 'y.w_id'
            box.schema.create_space('y')
            box.space.y:create_index('id',
                {type = 'tree', unique = true, parts = {
                    Y_ID_FN, 'string',
                }}
            )
            box.space.y:create_index('w_id',
                {type = 'tree', unique = false, parts = {
                    Y_W_ID_FN, 'string',
                }}
            )

            -- create 'z' and 'z.id'
            box.schema.create_space('z')
            box.space.z:create_index('id',
                {type = 'tree', unique = true, parts = {
                    Z_ID_FN, 'string',
                }}
            )
        end)
    end,

    fill_test_data = function(virtbox, meta)
        -- a    -> b
        -- ---------
        -- id_1 -> x
        -- id_2 -> x
        -- id_3 -> x
        -- id_4 -> x
        for i = 1, 4 do
            test_utils.replace_object(virtbox, meta, 'a', {
                id = ('id_%d'):format(i),
                b_id = 'x',
            })
        end
        test_utils.replace_object(virtbox, meta, 'b', {
            id = 'x',
        })

        -- w   -> x   -> z
        -- -----------------
        -- w_1 -> x_1 -> z_1
        -- w_1 -> x_2 -> z_2
        -- w_1 -> x_3 -> z_3
        -- w_1 -> x_4 -> z_4
        --
        -- w   -> y   -> z
        -- -----------------
        -- w_2 -> y_1 -> z_1
        -- w_2 -> y_2 -> z_2
        -- w_2 -> y_3 -> z_3
        -- w_2 -> y_4 -> z_4
        -- w_2 -> y_5 -> z_5
        test_utils.replace_object(virtbox, meta, 'w', {
            id = 'w_1',
        })
        test_utils.replace_object(virtbox, meta, 'w', {
            id = 'w_2',
        })
        for i = 1, 4 do
            test_utils.replace_object(virtbox, meta, 'x', {
                id = ('x_%d'):format(i),
                w_id = 'w_1',
                z_id = ('z_%d'):format(i),
            })
        end
        for i = 1, 5 do
            test_utils.replace_object(virtbox, meta, 'y', {
                id = ('y_%d'):format(i),
                w_id = 'w_2',
                z_id = ('z_%d'):format(i),
            })
        end
        for i = 1, 5 do
            test_utils.replace_object(virtbox, meta, 'z', {
                id = ('z_%d'):format(i),
            })
        end
    end,

    drop_spaces = function()
        box.space._schema:delete('oncegh-243-init-spaces')
        box.space.a:drop()
        box.space.b:drop()
        box.space.w:drop()
        box.space.x:drop()
        box.space.y:drop()
        box.space.z:drop()
    end,
}

local function run_queries(gql)
    local test = tap.test('do not fetch the same data twice')
    local is_cache_supported = test_utils.is_cache_supported()
    local on_bfs = test_utils.get_executor_name() == 'bfs'
    local check_statistics = is_cache_supported and on_bfs

    test:plan(2)

    -- 1 2 3 4
    -- \ | | /
    --  \\ //
    --    x
    test:test('same keys in a batch', function(test)
        test:plan(check_statistics and 2 or 1)

        local result = gql:compile([[
            {
                a {
                    id
                    b {id}
                }
            }
        ]]):execute()

        -- check the result data just in case
        test:is_deeply(result.data, {
            a = {
                {id = 'id_1', b = {id = 'x'}},
                {id = 'id_2', b = {id = 'x'}},
                {id = 'id_3', b = {id = 'x'}},
                {id = 'id_4', b = {id = 'x'}},
            }
        }, 'result data')

        -- check 'b' object fetched once: 'a': 4, 'b': 1
        if check_statistics then
            test:is(result.meta.statistics.fetched_object_cnt, 5, 'statistics')
        end
    end)

    -- w_1
    -- + x_1 x_2 x_3 x_4
    --    |   |   |   |
    --   z_1 z_2 z_3 z_4
    --
    -- w_2
    -- + y_1 y_2 y_3 y_4 y_5
    --    |   |   |   |   |
    --   z_1 z_2 z_3 z_4 z_5
    test:test('partially overlaping batches', function(test)
        test:plan(check_statistics and 2 or 1)

        local result = gql:compile([[
            {
                w {
                    id
                    x {
                        id
                        xz {id}
                    }
                    y {
                        id
                        yz {id}
                    }
                }
            }
        ]]):execute()

        -- check the result data just in case
        test:is_deeply(result.data, {
            w = {
                {
                    id = 'w_1',
                    x = {
                        {id = 'x_1', xz = {id = 'z_1'}},
                        {id = 'x_2', xz = {id = 'z_2'}},
                        {id = 'x_3', xz = {id = 'z_3'}},
                        {id = 'x_4', xz = {id = 'z_4'}},
                    },
                    y = {},
                },
                {
                    id = 'w_2',
                    x = {},
                    y = {
                        {id = 'y_1', yz = {id = 'z_1'}},
                        {id = 'y_2', yz = {id = 'z_2'}},
                        {id = 'y_3', yz = {id = 'z_3'}},
                        {id = 'y_4', yz = {id = 'z_4'}},
                        {id = 'y_5', yz = {id = 'z_5'}},
                    },
                },
            }
        }, 'result data')

        -- check each 'z' object fetched once: 'w': 2, 'x': 4, 'y': 5, 'z': 5
        if check_statistics then
            test:is(result.meta.statistics.fetched_object_cnt, 16, 'statistics')
        end
    end)

    assert(test:check(), 'check plan')
end

box.cfg({})

test_utils.run_testdata(testdata, {run_queries = run_queries})

os.exit()
