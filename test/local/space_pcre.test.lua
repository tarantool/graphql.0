#!/usr/bin/env tarantool

local fio = require('fio')

-- require in-repo version of graphql/ sources despite current working directory
package.path = fio.abspath(debug.getinfo(1).source:match("@?(.*/)")
    :gsub('/./', '/'):gsub('/+$', '')) .. '/../../?.lua' .. ';' ..
    package.path

local yaml = require('yaml')
local utils = require('graphql.utils')
local graphql = require('graphql')
local testdata = require('test.testdata.common_testdata')

-- helpers
-- -------

local function print_and_return(...)
    print(...)
    return table.concat({...}, ' ') .. '\n'
end

local function format_result(name, query, variables, result)
    return ('RUN %s {{{\nQUERY\n%s\nVARIABLES\n%s\nRESULT\n%s\n}}}\n'):format(
        name, query:rstrip(), yaml.encode(variables), yaml.encode(result))
end

-- init box, upload test data and acquire metadata
-- -----------------------------------------------

-- init box and data schema
box.cfg{background = false}
testdata.init_spaces()

-- upload test data
testdata.fill_test_data()

-- acquire metadata
local metadata = testdata.get_test_metadata()
local schemas = metadata.schemas
local collections = metadata.collections
local service_fields = metadata.service_fields
local indexes = metadata.indexes

-- build accessor and graphql schemas
-- ----------------------------------

local accessor = graphql.accessor_space.new({
    schemas = schemas,
    collections = collections,
    service_fields = service_fields,
    indexes = indexes,
})

local gql_wrapper = graphql.new({
    schemas = schemas,
    collections = collections,
    accessor = accessor,
})

-- run queries
-- -----------

local function run_queries(gql_wrapper)
    local results = ''

    local query_1 = [[
        query users($offset: String, $first_name_re: String,
                $middle_name_re: String) {
            user_collection(pcre: {first_name: $first_name_re,
                    middle_name: $middle_name_re}, offset: $offset) {
                first_name
                middle_name
                last_name
            }
        }
    ]]

    local gql_query_1 = gql_wrapper:compile(query_1)

    -- regexp match
    -- ------------

    utils.show_trace(function()
        local variables_1_1 = {
            first_name_re = '(?i)^i',
            middle_name_re = 'ich$',
        }
        local result = gql_query_1:execute(variables_1_1)
        results = results .. print_and_return(format_result(
            '1_1', query_1, variables_1_1, result))
    end)

    -- offset + regexp match
    -- ---------------------

    utils.show_trace(function()
        local variables_1_2 = {
            user_id = 'user_id_1',
            first_name_re = '^V',
        }
        local result = gql_query_1:execute(variables_1_2)
        results = results .. print_and_return(format_result(
            '1_2', query_1, variables_1_2, result))
    end)

    return results
end

run_queries(gql_wrapper)

-- clean up
-- --------

testdata.drop_spaces()

os.exit()
