#!/usr/bin/env tarantool

local fio = require('fio')

-- require in-repo version of graphql/ sources despite current working directory
package.path = fio.abspath(debug.getinfo(1).source:match("@?(.*/)")
    :gsub('/./', '/'):gsub('/+$', '')) .. '/../../?.lua' .. ';' .. package.path

local tap = require('tap')
local yaml = require('yaml')
local test_utils = require('test.test_utils')
local testdata = require('test.testdata.multihead_conn_testdata')

local function run_queries(gql_wrapper)
    local test = tap.test('aliases_multihead')
    test:plan(2)

    local query_1 = [[
        query obtainHero($hero_id: String!) {
            h: hero_collection(hero_id: $hero_id) {
                id: hero_id
                t: hero_type
                c: hero_connection {
                    ... on box_human_collection {
                        h: human_collection {
                            n: name
                        }
                    }
                    ... on box_starship_collection {
                        s: starship_collection {
                            m: model
                        }
                    }
                }
                b: banking_type
                bc: hero_banking_connection {
                    ... on box_array_credit_account_collection {
                        c: credit_account_collection {
                            a: account_id
                        }
                    }
                    ... on box_array_dublon_account_collection {
                        d: dublon_account_collection {
                            a: account_id
                        }
                    }
                }
            }
        }
    ]]

    local gql_query_1 = test_utils.show_trace(function()
        return gql_wrapper:compile(query_1)
    end)

    local exp_result_1_1 = yaml.decode(([[
        ---
        h:
        - id: hero_id_1
          t: human
          c:
            h:
              n: Luke
          b: credit
          bc:
            c:
            - a: credit_account_id_1
            - a: credit_account_id_2
            - a: credit_account_id_3
    ]]):strip())

    local result_1_1 = gql_query_1:execute({hero_id = 'hero_id_1'})
    test:is_deeply(result_1_1.data, exp_result_1_1,
        'query with aliases and multihead connections: human/credit')

    local exp_result_1_2 = yaml.decode(([[
        ---
        h:
        - id: hero_id_2
          t: starship
          c:
            s:
              m: Falcon-42
          b: dublon
          bc:
            d:
            - a: dublon_account_id_1
            - a: dublon_account_id_2
            - a: dublon_account_id_3
    ]]):strip())

    local result_1_2 = gql_query_1:execute({hero_id = 'hero_id_2'})
    test:is_deeply(result_1_2.data, exp_result_1_2,
        'query with aliases and multihead connections: starship/dublon')

    assert(test:check(), 'check plan')
end

box.cfg({})

test_utils.run_testdata(testdata, {run_queries = run_queries})

os.exit()
