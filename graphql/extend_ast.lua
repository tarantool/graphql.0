local expressions = require('graphql.expressions')

local extend_ast = {}

local function compile_filter_argument(node, context)
    assert(node.kind == 'argument')
    local argument_node = node
    local argument_name = argument_node.name.value

    if argument_name ~= 'filter' then return end

    -- save compiled expression to <string> node
    local value_node = argument_node.value
    if value_node.kind == 'variable' then
        -- a filter passed as a variable can use any other variables, so we disable
        -- this check for the operation with such filter
        local operation = context.currentOperation
        local operation_name = operation.name and operation.name.value or ''
        context.skipVariableUseCheck[operation_name] = true
        return
    end
    assert(value_node.kind == 'string' and type(value_node.value) == 'string',
        '"filter" list filtering argument must be a variable or a literal ' ..
        'string')
    local string_node = value_node
    local value = string_node.value
    local compiled_expr = expressions.new(value)
    string_node.compiled = compiled_expr

    -- XXX: don't blindly find the pattern {kind = 'variable', ...}, but either
    -- traverse a tree according to node kinds or export used variables info
    -- from expressions module

    -- mark used variables
    local open_set = {compiled_expr}
    while true do
        local e_node = table.remove(open_set, 1)
        if e_node == nil then break end
        if type(e_node) == 'table' then
            if e_node.kind == 'variable' then
                context.variableReferences[e_node.name] = true
            else
                for _, child_e_node in pairs(e_node) do
                    table.insert(open_set, child_e_node)
                end
            end
        end
    end
end

--- Visitors to pass to validate.lua.
function extend_ast.visitors()
    return {
        argument = {
            rules = {
                compile_filter_argument,
            }
        }
    }
end

return extend_ast
