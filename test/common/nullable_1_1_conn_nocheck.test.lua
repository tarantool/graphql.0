#!/usr/bin/env tarantool

local fio = require('fio')

-- require in-repo version of graphql/ sources despite current working directory
package.path = fio.abspath(debug.getinfo(1).source:match("@?(.*/)")
    :gsub('/./', '/'):gsub('/+$', '')) .. '/../../?.lua' .. ';' .. package.path

local tap = require('tap')
local yaml = require('yaml')
local test_utils = require('test.test_utils')
local testdata = require('test.testdata.nullable_1_1_conn_testdata')

box.cfg({})

local function run_queries(gql_wrapper)
    local test = tap.test('nullable_1_1_conn_nocheck')
    test:plan(1)

    local query_upside = [[
        query emails_trace_upside($body: String, $child_domain: String) {
            email(body: $body) {
                body
                in_reply_to(domain: $child_domain) {
                    body
                    in_reply_to {
                        body
                        in_reply_to {
                            body
                        }
                    }
                }
            }
        }
    ]]

    local gql_query_upside = gql_wrapper:compile(query_upside)

    -- Check we don't get an error re dangling 1:1 connection when
    -- `disable_dangling_check` is set.
    local variables_upside_z = {body = 'z'}
    local result = test_utils.show_trace(function()
        return gql_query_upside:execute(variables_upside_z)
    end)

    local exp_result = yaml.decode(([[
        ---
        email:
        - body: z
    ]]):strip())

    test:is_deeply(result.data, exp_result, 'upside_z disabled constraint check')

    assert(test:check(), 'check plan')
end

test_utils.run_testdata(testdata, {
    run_queries = run_queries,
    graphql_opts = {
        disable_dangling_check = true,
    },
})

os.exit()
