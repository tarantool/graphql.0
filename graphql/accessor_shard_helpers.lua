--- Auxiliary functions for shard module usage needed across several modules.

local json = require('json')

local accessor_shard_helpers = {}

function accessor_shard_helpers.shard_check_error(func_name, result, err)
    if result ~= nil then return end

    -- avoid json encoding of an error message (when the error is in the known
    -- format)
    if type(err) == 'table' and type(err.error) == 'string' then
        error({
            message = err.error,
            extensions = {
                shard_error = err,
            }
        })
    end

    error(('%s: %s'):format(func_name, json.encode(err)))
end

return accessor_shard_helpers
