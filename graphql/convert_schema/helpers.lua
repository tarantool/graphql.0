local utils = require('graphql.utils')
local check = utils.check

local helpers = {}

--- Get dot-separated name prepended with namespace.
---
--- @tparam string name base name
---
--- @tparam table context avro-schema parsing context to get namespace from
---
--- @treturn string full name
function helpers.full_name(name, context)
    check(name, 'name', 'string')
    check(context, 'context', 'table')
    check(context.path, 'context.path', 'table')

    if next(context.path) == nil then
        return name
    end

    local namespace = table.concat(context.path, '.')
    return namespace .. '.' .. name
end

--- Get last part of dot-separated name.
---
--- @tparam string name full name
---
--- @treturn string base name
function helpers.base_name(name)
    check(name, 'name', 'string')
    return name:gsub('^.*%.', '')
end

return helpers
