--- Various utility function used across the graphql module tests.

local yaml = require('yaml')

local utils = {}

function utils.format_result(name, query, variables, result)
    return ('RUN %s {{{\nQUERY\n%s\nVARIABLES\n%s\nRESULT\n%s\n}}}\n'):format(
    name, query:rstrip(), yaml.encode(variables), yaml.encode(result))
end

function utils.print_and_return(...)
    print(...)
    return table.concat({ ... }, ' ') .. '\n'
end

return utils
