#!/usr/bin/env tarantool

local expressions = require('graphql.expressions')
local tap = require('tap')
local test = tap.test('expr_tree')

-- Some of possible identifiers for tests. In case you want to
-- add any new identifiers for testing just add them here to the
-- corresponding tables.
local identifiers = {
    {'const', 'false', 'bool'},
    {'const', 'true', 'bool'},
    {'const', '11', 'number'},
    {'const', '22.8', 'number'},
    {'const', 'something', 'string'},
    {'variable', '$variable'},
    {'field_path', 'some_path.to.this.field'},
}
local ID_TYPE = 3

local binary_operators = {
    '||',
    '&&',
    '+',
    '-',
    '==',
    '!=',
    '>',
    '>=',
    '<',
    '<=',
}

test:plan(1046)

-- Bunch of tests for all the sets of binary operator and its
-- operands.
for i_1, id_1 in ipairs(identifiers) do
    for i_2, id_2 in ipairs(identifiers) do
        for _, op in ipairs(binary_operators) do
            local first_identifier = id_1[2]
            local second_identifier = id_2[2]
            if id_1[ID_TYPE] == 'string' then
                first_identifier = ('"%s"'):format(first_identifier)
            end
            if id_2[ID_TYPE] == 'string' then
                second_identifier = ('"%s"'):format(second_identifier)
            end
            local bin_operation = ('%s %s %s'):format(first_identifier, op,
                                                      second_identifier)
            local ast = expressions.parse(bin_operation)
            local node_1
            local node_2

            if id_1[1] == 'const' then
                node_1 = {
                    kind = 'const',
                    value_class = id_1[ID_TYPE],
                    value = id_1[2]
                }
            end
            if id_1[1] == 'variable' then
                node_1 = {
                    kind = 'variable',
                    name = id_1[2]:gsub('%$', '')
                }
            end
            if id_1[1] == 'field_path' then
                node_1 = {kind = 'object_field', path = id_1[2]}
            end

            if id_2[1] == 'const' then
                node_2 = {
                    kind = 'const',
                    value_class = id_2[ID_TYPE],
                    value = id_2[2]
                }
            end
            if id_2[1] == 'variable' then
                node_2 = {
                    kind = 'variable',
                    name = id_2[2]:gsub('%$', '')
                }
            end
            if id_2[1] == 'field_path' then
                node_2 = {kind = 'object_field', path = id_2[2]}
            end

            local op_node = {
                kind = 'binary_operations',
                operators = {op},
                operands = {node_1, node_2}
            }
            local expected = {kind = 'root_expression', expr = op_node}
            local test_name = ('ast_bin_op_1_%d.%s.%d'):format(i_1, op, i_2)
            test:is_deeply(ast, expected, test_name)

            bin_operation = ('!(%s %s %s)'):format(first_identifier, op,
                                                   second_identifier)
            ast = expressions.parse(bin_operation)
            expected = {
                kind = 'root_expression',
                expr = {
                    kind = 'unary_operation',
                    op = '!',
                    node = op_node
                } }
            test_name = ('ast_bin_op_2_%d.%s.%d'):format(i_1, op, i_2)
            test:is_deeply(ast, expected, test_name)
        end
    end
end

-- Bunch of tests for existing functions operands.
for i_1, id_1 in ipairs(identifiers) do
    local first_identifier = id_1[2]
    if id_1[ID_TYPE] == 'string' then
        first_identifier = ('"%s"'):format(first_identifier)
    end
    local func = ('is_null( %s )'):format(first_identifier)
    local ast = expressions.parse(func)
    local arg_node_1
    local arg_node_2
    if id_1[1] == 'const' then
        arg_node_1 = {
            kind = 'const',
            value_class = id_1[ID_TYPE],
            value = id_1[2]
        }
    end
    if id_1[1] == 'variable' then
        arg_node_1 = {
            kind = 'variable',
            name = id_1[2]:gsub('%$', '')
        }
    end
    if id_1[1] == 'field_path' then
        arg_node_1 = {kind = 'object_field', path = id_1[2]}
    end
    local expected = {
        kind = 'root_expression',
        expr = {
            kind = 'func',
            name = 'is_null',
            args = {arg_node_1}
        }
    }
    local test_name = ('ast_func_is_null_%d'):format(i_1)
    test:is_deeply(ast, expected, test_name)

    func = ('not_null( %s )'):format(first_identifier)
    ast = expressions.parse(func)
    expected = {
        kind = 'root_expression',
        expr = {
            kind = 'func',
            name = 'not_null',
            args = {arg_node_1}
        }
    }
    test_name = ('ast_func_not_null_%d'):format(i_1)
    test:is_deeply(ast, expected, test_name)

    for i_2, id_2 in ipairs(identifiers) do
        local second_identifier = id_2[2]
        if id_2[ID_TYPE] == 'string' then
            second_identifier = ('"%s"'):format(second_identifier)
        end
        func = ('regexp( %s , %s )'):format(first_identifier, second_identifier)
        ast = expressions.parse(func)
        if id_2[1] == 'const' then
            arg_node_2 = {
                kind = 'const',
                value_class = id_2[ID_TYPE],
                value = id_2[2]
            }
        end
        if id_2[1] == 'variable' then
            arg_node_2 = {
                kind = 'variable',
                name = id_2[2]:gsub('%$', '')
            }
        end
        if id_2[1] == 'field_path' then
            arg_node_2 = {kind = 'object_field', path = id_2[2]}
        end
        expected = {
            kind = 'root_expression',
            expr = {
                kind = 'func',
                name = 'regexp',
                args = {arg_node_1, arg_node_2}
            }
        }
        test_name = ('ast_func_regexp_%d.%d'):format(i_1, i_2)
        test:is_deeply(ast, expected, test_name)
    end
end

local ast = expressions.parse('true && ($variable + 7 <= field.path_1 || ' ..
                              '!("abc" > "abd")) && !false')
local expected = {
    kind = 'root_expression',
    expr = {
        kind = 'binary_operations',
        operators = {'&&', '&&'},
        operands = {
            {kind = 'const', value_class = 'bool', value = 'true'},
            {
                kind = 'binary_operations',
                operators = {'||'},
                operands = {
                    {
                        kind = 'binary_operations',
                        operators = {'<='},
                        operands = {
                            {
                                kind = 'binary_operations',
                                operators = {'+'},
                                operands = {
                                    {
                                        kind = 'variable',
                                        name = 'variable'
                                    },
                                    {
                                        kind = 'const',
                                        value_class = 'number',
                                        value = '7'
                                    }
                                }
                            },
                            {
                                kind = 'object_field',
                                path = 'field.path_1'
                            }
                        }
                    },
                    {
                        kind = 'unary_operation',
                        op = '!',
                        node = {
                            kind = 'binary_operations',
                            operators = {'>'},
                            operands = {
                                {
                                    kind = 'const',
                                    value_class = 'string',
                                    value = 'abc'
                                },
                                {
                                    kind = 'const',
                                    value_class = 'string',
                                    value = 'abd'
                                }
                            }
                        }
                    }
                }
            },
            {
                kind = 'unary_operation',
                op = '!',
                node = {kind = 'const', value_class = 'bool', value = 'false'}
            }
        }
    }
}
test:is_deeply(ast, expected, 'ast_handwritten_test_1')


-- Testing if the same priority binary operators in the "same
-- amount of brackets" are actually different nodes.
ast = expressions.parse('(!false|| true) && (!(true)              || false)')
expected = {
    kind = 'root_expression',
    expr = {
        kind = 'binary_operations',
        operators = {'&&'},
        operands = {
            {
                kind = 'binary_operations',
                operators = {'||'},
                operands = {
                    {
                        kind = 'unary_operation',
                        op = '!',
                        node = {
                            kind = 'const',
                            value_class = 'bool',
                            value = 'false'
                        }
                    },
                    {
                        kind = 'const',
                        value_class = 'bool',
                        value = 'true'
                    }
                }
            },
            {
                kind = 'binary_operations',
                operators = {'||'},
                operands = {
                    {
                        kind = 'unary_operation',
                        op = '!',
                        node = {
                            kind = 'const',
                            value_class = 'bool',
                            value = 'true'
                        }
                    },
                    {
                        kind = 'const',
                        value_class = 'bool',
                        value = 'false'
                    }
                }
            }
        }
    }
}
test:is_deeply(ast, expected, 'ast_handwritten_test_2')

ast = expressions.parse('false && "hello there" && 72.1 && $variable && ' ..
                        '_object1__.field && is_null($non_nil_variable) && ' ..
                        'regexp("pattern", "string")')
expected = {
    kind = 'root_expression',
    expr = {
        kind = 'binary_operations',
        operators = {'&&', '&&', '&&', '&&', '&&', '&&'},
        operands = {
            {
                kind = 'const',
                value_class = 'bool',
                value = 'false'
            },
            {
                kind = 'const',
                value_class = 'string',
                value = 'hello there'
            },
            {
                kind = 'const',
                value_class = 'number',
                value = '72.1'
            },
            {
                kind = 'variable',
                name = 'variable'
            },
            {
                kind = 'object_field',
                path = '_object1__.field'
            },
            {
                kind = 'func',
                name = 'is_null',
                args = {
                    {
                        kind = 'variable',
                        name = 'non_nil_variable'
                    }
                }
            },
            {
                kind = 'func',
                name = 'regexp',
                args = {
                    {
                        kind = 'const',
                        value_class = 'string',
                        value = 'pattern'
                    },
                    {
                        kind = 'const',
                        value_class = 'string',
                        value = 'string'
                    }
                }
            }
        }
    }
}

test:is_deeply(ast, expected, 'ast_handwritten_test_3')
