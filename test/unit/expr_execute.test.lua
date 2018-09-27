#!/usr/bin/env tarantool

local expressions = require('graphql.expressions')
local tap = require('tap')
local test = tap.test('expr_tree')

test:plan(8)

-- case: simple
local case_name = 'simple'
local str = '2 + 2'
local exp_result = 4
local expr = expressions.new(str)
test:is(expr:execute(), exp_result, case_name)
test:is(expressions.execute(str), exp_result, case_name)

local object = {
    node_1 = {
        node_2 = 21
    },
    field = {
        path_1 = 28
    }
}
local variables = {
    variable = 21,
}

-- case: complex
local case_name = 'complex'
local str = 'true && ($variable + 1 * 7 == field.path_1 && !(2399>23941)) && !false'
local exp_result = true
local expr = expressions.new(str)
test:is(expr:execute(object, variables), exp_result, case_name)
test:is(expressions.execute(str, object, variables), exp_result, case_name)

local object = {
    path = {
        ddf = 'hi_there',
    }
}
local variables = {
    var = 'abc',
}

-- case: regexp, is_null, is_not_null
local case_name = 'regexp, is_null, is_not_null'
local str = 'false || (regexp("abc", $var)) && (is_null(path.ddf))||' ..
            ' is_not_null(path.ddf)'
local exp_result = true
local expr = expressions.new(str)
test:is(expr:execute(object, variables), exp_result, case_name)
test:is(expressions.execute(str, object, variables), exp_result, case_name)

-- case: regexp, is_null
local case_name = 'regexp, is_null'
local str = 'false || (regexp("abc", $var)) && (is_null(path.ddf))|| is_null(path.ddf)'
local expr = expressions.new(str)
local exp_result = false
test:is(expr:execute(object, variables), exp_result, case_name)
test:is(expressions.execute(str, object, variables), exp_result, case_name)

os.exit(test:check() == true and 0 or 1)
