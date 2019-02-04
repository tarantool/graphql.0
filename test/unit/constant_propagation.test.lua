#!/usr/bin/env tarantool

local tap = require('tap')
local fio = require('fio')
local json = require('json')

-- require in-repo version of graphql/ sources despite current working directory
local cur_dir = fio.abspath(debug.getinfo(1).source:match("@?(.*/)")
    :gsub('/./', '/'):gsub('/+$', ''))
package.path =
    cur_dir .. '/../../?/init.lua' .. ';' ..
    cur_dir .. '/../../?.lua' .. ';' ..
    package.path

local expressions = require('graphql.expressions')
local constant_propagation = require('graphql.expressions.constant_propagation')
local test_utils = require('test.test_utils')

local cases = {
    {
        expr = 'x > 7 + $v + 1',
        variables = {v = 2},
        expected_ast = {
            kind = 'binary_operation',
            op = '>',
            left = {
                kind = 'object_field',
                path = 'x',
            },
            right = {
                kind = 'evaluated',
                value = 10,
            },
        },
        execute = {
            {
                object = {x = 10},
                expected_result = false,
            },
            {
                object = {x = 12},
                expected_result = true,
            },
        }
    },
    {
        expr = 'true && x > 1',
        expected_ast = {
            kind = 'binary_operation',
            op = '>',
            left = {
                kind = 'object_field',
                path = 'x',
            },
            right = {
                kind = 'evaluated',
                value = 1,
            },
        },
        execute = {
            {
                object = {x = 1},
                expected_result = false,
            },
            {
                object = {x = 2},
                expected_result = true,
            },
        }
    },
    {
        expr = 'false && x > 1',
        expected_ast = {
            kind = 'evaluated',
            value = false,
        },
        execute = {
            {
                object = {x = 1},
                expected_result = false,
            },
            {
                object = {x = 2},
                expected_result = false,
            },
        }
    },
    {
        expr = '1 + 2 * $v == -1',
        variables = {v = -1},
        expected_ast = {
            kind = 'evaluated',
            value = true,
        },
        execute = {
            {
                expected_result = true,
            },
        }
    },
}

local function run_case(test, case)
    test:test(case.name or case.expr, function(test)
        local plan = 1
        if case.expected_ast then
            plan = plan + 1
        end
        plan = plan + #(case.execute or {})
        test:plan(plan)

        local compiled_expr = expressions.new(case.expr)
        local context = {variables = case.variables}
        local optimized_expr = constant_propagation.transform(compiled_expr,
            context)
        if case.expected_ast then
            test:is_deeply(optimized_expr.ast, case.expected_ast,
                'optimized ast')
        end
        for _, e in ipairs(case.execute or {}) do
            local result = optimized_expr:execute(e.object)
            test:is(result, e.expected_result, 'execute with ' ..
                json.encode(e.object))
        end
        local optimized_expr_2 = constant_propagation.transform(optimized_expr,
            context)
        test:ok(optimized_expr == optimized_expr_2, 'self-applicability')
    end)
end

local test = tap.test('constant_propagation')
test:plan(#cases)

for _, case in ipairs(cases) do
    test_utils.show_trace(run_case, test, case)
end

os.exit(test:check() == true and 0 or 1)
