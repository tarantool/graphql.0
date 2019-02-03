#!/usr/bin/env tarantool

local fio = require('fio')

-- require in-repo version of graphql/ sources despite current working directory
package.path =
    fio.abspath(debug.getinfo(1).source:match("@?(.*/)")
    :gsub('/./', '/'):gsub('/+$', '') .. '/../?.lua') .. ';' ..
    fio.abspath(debug.getinfo(1).source:match("@?(.*/)")
    :gsub('/./', '/'):gsub('/+$', '') .. '/../?/init.lua') .. ';' ..
    package.path

local log = require('log')
local test_utils = require('test.test_utils')

-- e. g. nullable_1_1_conn
local testdata_name = arg[1] or 'common'
-- e. g. test/testdata/nullable_1_1_conn_testdata.lua
if testdata_name:find('/') then
    testdata_name = testdata_name:gsub('^.+/(.+)_testdata.lua$', '%1')
end

local testdata = require(('test.testdata.%s_testdata'):format(testdata_name))

local function start()
    box.cfg({})
    local ok, err = pcall(function()
        testdata.drop_spaces()
    end)
    if not ok then
        log.warn('Cannot drop data: ' .. tostring(err))
    end
    local meta = testdata.meta or testdata.get_test_metadata()
    testdata.init_spaces()
    testdata.fill_test_data(box.space, meta)
    local gql_wrapper = test_utils.graphql_from_testdata(testdata)
    local msg = gql_wrapper:start_server()
    log.info(tostring(msg))
end

start()
