#!/usr/bin/env tarantool

local fio = require('fio')

-- require in-repo version of graphql/ sources despite current working directory
package.path = fio.abspath(debug.getinfo(1).source:match("@?(.*/)")
    :gsub('/./', '/'):gsub('/+$', '')) .. '/../../?.lua' .. ';' .. package.path

local tap = require('tap')
--local yaml = require('yaml')
local test_utils = require('test.test_utils')
local testdata = require('test.testdata.common_testdata')

local function run_queries(gql_wrapper)
    local test = tap.test('directives')
    test:plan(0)
    assert(test:check(), 'check plan')
end


box.cfg({})

test_utils.run_testdata(testdata, {
    run_queries = run_queries,
})

os.exit()
