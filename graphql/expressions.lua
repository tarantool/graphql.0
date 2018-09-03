local lpeg = require('lulpeg')

local expressions = {}
local P, R, S, V = lpeg.P, lpeg.R, lpeg.S, lpeg.V
local C = lpeg.C

-- Some special symbols.
local space_symbol = S(' \t\r\n')
local eof = P(-1)
local spaces = space_symbol ^ 0

-- Possible identifier patterns:
--   1) Number.
local digit = R('09')
local integer = R('19') * digit ^ 0
local decimal = (digit ^ 1) * '.' * (digit ^ 1)
local number = decimal + integer
--   2) Boolean.
local bool = P('false') + P('true')
--   3) String.
local string = P('"') * C((P('\\"') + 1 - P('"')) ^ 0) * P('"')
--   4) Variable.
local identifier = ('_' + R('az', 'AZ')) * ('_' + R('09', 'az', 'AZ')) ^ 0
local variable_name = identifier
--   5) Object's field path.
local field_path = identifier * ('.' * identifier) ^ 0

-- Possible logical function patterns:
--   1) is_null.
local is_null = P('is_null')
--   2) not_null.
local not_null = P('not_null')
--   3) regexp.
local regexp = P('regexp')

-- Possible unary operator patterns:
--   1) Logical negation.
local negation = P('!')
--   2) Unary minus.
local unary_minus = P('-')
--   3) Unary plus.
local unary_plus = P('+')

-- Possible binary operator patterns:
--   1) Logical and.
local logic_and = P('&&')
--   2) logical or.
local logic_or = P('||')
--   3) +
local addition = P('+')
--   4) -
local subtraction = P('-')
--   5) ==
local eq = P('==')
--   6) !=
local not_eq = P('!=')
--   7) >
local gt = P('>')
--   8) >=
local ge = P('>=')
--   9) <
local lt = P('<')
--   10) <=
local le = P('<=')

--- Utility functions:
---     1) Vararg iterator.


local function vararg(...)
    local i = 0
    local t = {}
    local limit
    local function iter(...)
        i = i + 1
        if i > limit then return end
        return i, t[i]
    end

    i = 0
    limit = select("#", ...)
    for n = 1, limit do
        t[n] = select(n, ...)
    end
    for n = limit + 1, #t do
        t[n] = nil
    end
    return iter
end

-- AST nodes generating functions.
local function identical(arg)
    return arg
end

local identical_node = identical

local op_name = identical

local function root_expr_node(expr)
    return {
        kind = 'root_expression',
        expr = expr
    }
end

local function bin_op_node(...)
    if select('#', ...) == 1 then
        return select(1, ...)
    end
    local operators = {}
    local operands = {}
    for i, v in vararg(...) do
        if i % 2 == 0 then
            table.insert(operators, v)
        else
            table.insert(operands, v)
        end
    end
    return {
        kind = 'binary_operations',
        operators = operators,
        operands = operands
    }
end

local function unary_op_node(unary_operator, operand_1)
    return {
        kind = 'unary_operation',
        op = unary_operator,
        node = operand_1
    }
end

local function func_node(name, ...)
    local args = {...}
    return {
        kind = 'func',
        name = name,
        args = args
    }
end

local function number_node(value)
    return {
        kind = 'const',
        value_class = 'number',
        value = value
    }
end

local function string_node(value)
    return {
        kind = 'const',
        value_class = 'string',
        value = value
    }
end

local function bool_node(value)
    return {
        kind = 'const',
        value_class = 'bool',
        value = value
    }
end

local function variable_node(name)
    return {
        kind = 'variable',
        name = name
    }
end

local function path_node(path)
    return {
        kind = 'object_field',
        path = path
    }
end

-- Patterns returning corresponding nodes (note that all of them
-- start with '_').
local _number = number / number_node
local _bool = bool / bool_node
local _string = string / string_node
local _variable = '$' * C(variable_name) / variable_node
local _field_path = field_path / path_node
local _literal = _bool + _number + _string

local _logic_or = logic_or / op_name
local _logic_and = logic_and / op_name
local _comparison_op = (eq + not_eq + ge + gt + le + lt) / op_name
local _arithmetic_op = (addition + subtraction) / op_name
local _unary_op = (negation + unary_minus + unary_plus) / op_name
local _functions = (is_null + not_null + regexp) / identical

-- Grammar rules for C-style expressions positioned ascending in
-- terms of priority.
local expression_grammar = P {
    'init_expr',
    init_expr = V('expr') * eof / root_expr_node,
    expr = spaces * V('log_expr_or') * spaces / identical_node,

    log_expr_or = V('log_expr_and') * (spaces * _logic_or *
                  spaces * V('log_expr_and')) ^ 0 / bin_op_node,
    log_expr_and = V('comparison') * (spaces * _logic_and * spaces *
                   V('comparison')) ^ 0 / bin_op_node,
    comparison = V('arithmetic_expr') * (spaces * _comparison_op * spaces *
                 V('arithmetic_expr')) ^ 0 / bin_op_node,
    arithmetic_expr = V('unary_expr') * (spaces * _arithmetic_op * spaces *
                      V('unary_expr')) ^ 0 / bin_op_node,

    unary_expr = (_unary_op * V('first_prio') / unary_op_node) +
                 (V('first_prio') / identical_node),
    first_prio = (V('func') + V('value_terminal') + '(' * spaces * V('expr') *
                  spaces * ')') / identical_node,
    func = _functions * '(' * spaces * V('value_terminal') * (spaces * ',' *
           spaces * V('value_terminal')) ^ 0 * spaces * ')' / func_node,
    value_terminal = (_literal + _variable + _field_path) / identical_node
}

--- Parse given string which supposed to be a c-style expression.
---
--- @tparam str string representation of expression.
---
--- @treturn syntax tree.
function expressions.parse(str)
    assert(type(str) == 'string', 'parser expects a string')
    return expression_grammar:match(str) or error('syntax error')
end

return expressions
