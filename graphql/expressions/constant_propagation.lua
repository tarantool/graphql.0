--- Constant propagation optimization pass.

local expressions = require('graphql.expressions')

local constant_propagation = {}

local function evaluate_node(node, context)
    local value = expressions.execute_node(node, context)
    return {
        kind = 'evaluated',
        value = value,
    }
end

--- Fold variables and constant expressions into constants.
---
--- Introdices the new node type:
---
--- {
---     kind = 'evaluated',
---     value = <...> (of any type),
--- }
---
--- @tparam table node current AST node (e.g. root node)
---
--- @tparam table context table of the following values:
---
--- * variables (table)
---
--- @treturn table transformed AST
local function propagate_constants_internal(node, context)
    if node.kind == 'const' or node.kind == 'variable' then
        return evaluate_node(node, context)
    elseif node.kind == 'object_field' then
        return node
    elseif node.kind == 'func' then
        local new_args = {}
        local changed = false -- at least one arg was changed
        local evaluated = true -- all args were evaluated
        for i = 1, #node.args do
            local arg = propagate_constants_internal(node.args[i], context)
            table.insert(new_args, arg)
            if arg ~= node.args[i] then
                changed = true
            end
            if arg.kind ~= 'evaluated' then
                evaluated = false
            end
        end
        if not changed then return node end
        local new_node = {
            kind = 'func',
            name = node.name,
            args = new_args,
        }
        if not evaluated then return new_node end
        return evaluate_node(new_node, context)
    elseif node.kind == 'unary_operation' then
        local arg = propagate_constants_internal(node.node, context)
        if arg == node.node then return node end
        local new_node = {
            kind = 'unary_operation',
            op = node.op,
            node = arg,
        }
        if arg.kind ~= 'evaluated' then return new_node end
        return evaluate_node(new_node, context)
    elseif node.kind == 'binary_operation' then
        local left = propagate_constants_internal(node.left, context)
        local right = propagate_constants_internal(node.right, context)

        if left == node.left and right == node.right then return node end

        -- handle the case when both args were evaluated
        if left.kind == 'evaluated' and right.kind == 'evaluated' then
            return evaluate_node({
                kind = 'binary_operation',
                op = node.op,
                left = left,
                right = right,
            }, context)
        end

        -- handle '{false,true} {&&,||} X' and 'X {&&,||} {false,true}'
        if node.op == '&&' or node.op == '||' then
            local e_node -- evaluated node
            local o_node -- other node

            if left.kind == 'evaluated' then
                e_node = left
                o_node = right
            elseif right.kind == 'evaluated' then
                e_node = right
                o_node = left
            end

            if e_node ~= nil then
                assert(type(e_node.value) == 'boolean')
                if node.op == '&&' and e_node.value then
                    -- {true && o_node, o_node && true}
                    return o_node
                elseif node.op == '&&' and not e_node.value then
                    -- {false && o_node, o_node && false}
                    return {
                        kind = 'evaluated',
                        value = false,
                    }
                elseif node.op == '||' and e_node.value then
                    -- {true || o_node, o_node || true}
                    return {
                        kind = 'evaluated',
                        value = true,
                    }
                elseif node.op == '||' and not e_node.value then
                    -- {false || o_node, o_node || false}
                    return o_node
                else
                    assert(false)
                end
            end
        end

        return {
            kind = 'binary_operation',
            op = node.op,
            left = left,
            right = right,
        }
    elseif node.kind == 'evaluated' then
        return node
    else
        error('Unknown node kind: ' .. tostring(node.kind))
    end
end

--- Fold variables and constant expressions into constants.
---
--- Introdices the new node type:
---
--- {
---     kind = 'evaluated',
---     value = <...> (of any type),
--- }
---
--- @tparam table expr expression
---
--- @tparam table context table of the following values:
---
--- * variables (table)
---
--- @treturn table transformed expression
function constant_propagation.transform(expr, context)
    local new_ast = propagate_constants_internal(expr.ast, context)
    if new_ast == expr.ast then return expr end
    return setmetatable({
        raw = expr.raw,
        ast = new_ast,
    }, getmetatable(expr))
end

return constant_propagation
