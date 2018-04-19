#!/usr/bin/env tarantool

local fio = require('fio')

-- require in-repo version of graphql/ sources despite current working directory
package.path = fio.abspath(debug.getinfo(1).source:match("@?(.*/)")
    :gsub('/./', '/'):gsub('/+$', '')) .. '/../../?.lua' .. ';' .. package.path

local utils = require('test.utils')
local testdata = require('test.testdata.compound_index_testdata')

box.cfg({})

utils.run_testdata(testdata)

os.exit()
