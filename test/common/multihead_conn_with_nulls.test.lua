#!/usr/bin/env tarantool

local fio = require('fio')

-- require in-repo version of graphql/ sources despite current working directory
package.path = fio.abspath(debug.getinfo(1).source:match("@?(.*/)")
    :gsub('/./', '/'):gsub('/+$', '')) .. '/../../?.lua' .. ';' .. package.path

local test_utils = require('test.test_utils')
local testdata = require('test.testdata.multihead_conn_with_nulls_testdata')

box.cfg({})

test_utils.run_testdata(testdata)

os.exit()
