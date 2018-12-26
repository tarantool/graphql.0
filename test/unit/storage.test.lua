#!/usr/bin/env tarantool

local fio = require('fio')

-- require in-repo version of graphql/ sources despite current working directory
local cur_dir = fio.abspath(debug.getinfo(1).source:match("@?(.*/)")
    :gsub('/./', '/'):gsub('/+$', ''))
package.path =
    cur_dir .. '/../../?/init.lua' .. ';' ..
    cur_dir .. '/../../?.lua' .. ';' ..
    package.path

local tap = require('tap')
local json = require('json')
local fun = require('fun')
local storage = require('graphql.storage')

local function get_all_chunks(space_name, index_name, key, opts)
    local res = {}
    local steps = {} -- to debug fails

    local metainfo
    local cursor
    local data
    repeat
        metainfo, data = single_select(space_name, index_name, key, opts, cursor)
        cursor = metainfo.cursor
        for _, tuple in ipairs(data) do
            table.insert(res, tuple)
        end
        table.insert(steps, {
            metainfo = metainfo,
            data = data,
        })
    until cursor.is_end

    return res, steps
end

local function test_single_select(test, space_name, index_name, key, opts,
        block_size)
    local name = json.encode({space_name, index_name, key, opts,
        'block_size ' .. tostring(block_size)})
    local exp = box.space[space_name].index[index_name]:select(key, opts)
    local res, steps = get_all_chunks(space_name, index_name, key, opts)

    -- prepare to compare: convert tuples to tables
    exp = fun.iter(exp):map(box.tuple.totable):totable()
    res = fun.iter(res):map(box.tuple.totable):totable()

    local ok = test:is_deeply(res, exp, name, {
        res = res,
        exp = exp,
        steps = steps,
    })
end

local cases = {
    {
        space_name = 'test',
        index_name = 'pk',
        key = {},
        opts = {},
    },
    {
        space_name = 'test',
        index_name = 'pk',
        key = {1},
        opts = {},
    },
    {
        space_name = 'test',
        index_name = 'pk',
        key = {},
        opts = {limit = 1},
    },
    {
        space_name = 'test',
        index_name = 'pk',
        key = {},
        opts = {limit = 1, offset = 1},
    },
    {
        space_name = 'test',
        index_name = 'pk',
        key = {1},
        opts = {iterator = 'GT', limit = 4},
    },
    {
        space_name = 'test',
        index_name = 'sk',
        key = {},
        opts = {},
    },
    {
        space_name = 'test',
        index_name = 'sk',
        key = {'c'},
        opts = {},
    },
    {
        space_name = 'test',
        index_name = 'sk',
        key = {'b'},
        opts = {iterator = 'GT'},
    },
    {
        space_name = 'test',
        index_name = 'pk',
        key = {999}, -- non-exists
        opts = {},
    },
    {
        space_name = 'test',
        index_name = 'sk',
        key = {'zzz'}, -- non-exists
        opts = {},
    },
}

local test = tap.test('storage')
test:plan(10 * #cases)

box.cfg({})
box.schema.create_space('test')
box.space.test:create_index('pk', {
    type = 'tree',
    unique = true,
    parts = {
        {field = 1, type = 'unsigned'},
    },
})
box.space.test:create_index('sk', {
    type = 'tree',
    unique = false,
    parts = {
        {field = 2, type = 'string', is_nullable = true},
    },
})

box.space.test:insert({1, 'a'})
box.space.test:insert({2, 'b'})
box.space.test:insert({3, 'c'})
box.space.test:insert({4, 'c'})
box.space.test:insert({5, 'c'})
box.space.test:insert({6, 'c'})
box.space.test:insert({7, 'c'})
box.space.test:insert({8, 'c'})
box.space.test:insert({9, 'c'})
box.space.test:insert({10, 'c'})
box.space.test:insert({11, 'c'})
box.space.test:insert({12, 'c'})
box.space.test:insert({13, 'd'})
box.space.test:insert({14, 'e'})
box.space.test:insert({15, 'f'})
box.space.test:insert({16, 'g'})

storage.init()

for block_size = 1, 10 do
    for _, case in ipairs(cases) do
        storage.set_block_size(block_size)
        test_single_select(test, case.space_name, case.index_name, case.key,
            case.opts, block_size)
    end
end

box.space.test:drop()
os.exit(test:check() == true and 0 or 1)
