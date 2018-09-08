#!/usr/bin/env tarantool

-- https://github.com/tarantool/graphql/issues/218

local fio = require('fio')
local tap = require('tap')

-- require in-repo version of graphql/ sources despite current working directory
local cur_dir = fio.abspath(debug.getinfo(1).source:match("@?(.*/)")
    :gsub('/./', '/'):gsub('/+$', ''))
package.path =
    cur_dir .. '/../../?/init.lua' .. ';' ..
    cur_dir .. '/../../?.lua' .. ';' ..
    package.path

-- require no_shard/shard.lua instead of the real shard module
package.path =
    cur_dir .. '/no_shard/?/init.lua' .. ';' ..
    cur_dir .. '/no_shard/?.lua' .. ';' ..
    package.path

local graphql = require('graphql')
local testdata = require('test.testdata.common_testdata')

local test = tap.test('optional shard')
test:plan(1)

box.cfg{wal_mode='none'}

local cfg = testdata.get_test_metadata()
cfg.accessor = 'space'
local ok = pcall(graphql.new, cfg)
test:ok(ok, 'shard is optional')

os.exit(test:check() == true and 0 or 1)
