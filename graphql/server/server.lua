local fio = require('fio')
local utils = require('graphql.server.utils')
local json = require('json')

local server = {}

local default_charset = "utf-8"

local function file_mime_type(filename)
    if string.endswith(filename, ".css") then
        return string.format("text/css; charset=%s", default_charset)
    elseif string.endswith(filename, ".js") then
        return string.format("application/javascript; charset=%s", default_charset)
    elseif string.endswith(filename, ".html") then
        return string.format("text/html; charset=%s", default_charset)
    elseif string.endswith(filename, ".svg") then
        return string.format("image/svg+xml")
    end

    return "application/octet-stream"
end

local function static_handler(req)
    local path = req.path

    if path == '/' then
        path = fio.pathjoin('graphiql', 'index.html')
    else
        path = fio.pathjoin('graphiql', path)
    end

    local lib_dir = utils.script_path()
    path = fio.pathjoin(lib_dir, path)
    local body = utils.read_file(path)

    return {
        status = 200,
        headers = {
            ['content-type'] = file_mime_type(path)
        },
        body = body
    }
end

function server.init(graphql, host, port)
    local host = host or '127.0.0.1'
    local port = port or 8080
    local httpd = require('http.server').new(host, port)

    local function api_handler(req)
        local body = req:read()

        if body == nil or body == '' then
            return {
                status = 200,
                body = json.encode(
                    {errors = {{message = "Expected a non-empty request body"}}}
                )
            }
        end

        local parsed = json.decode(body)
        if parsed == nil then
            return {
                status = 200,
                body = json.encode(
                    {errors = {{message = "Body should be a valid JSON"}}}
                )
            }
        end

        if type(parsed) ~= 'table' then
            return {
                status = 200,
                body = json.encode(
                    {errors = {message = "Body should be a dictionary"}}
                )
            }
        end

        if parsed.query == nil or type(parsed.query) ~= "string" then
            return {
                status = 200,
                body = json.encode(
                    {errors = {{message = "Body should have 'query' field"}}}
                )
            }
        end

        local variables = parsed.variables
        if variables == nil then
            variables = {}
        end

        if type(variables) == 'cdata' then
            if variables == nil then
                variables = {}
            end
        end

        local query = parsed.query

        local ok, compiled_query = pcall(graphql.compile, graphql, query)
        if not ok then
            return {
                status = 200,
                body = json.encode({errors = {{message = compiled_query}}})
            }
        end

        local operation_name = parsed.operationName
        -- box.NULL -> nil
        if operation_name == nil then
            operation_name = nil
        end

        local ok, result = pcall(compiled_query.execute, compiled_query,
            variables, operation_name)
        if not ok then
            return {
                status = 200,
                body = json.encode({error = {{message = result}}})
            }
        end

        result = {data = result}
        return {
            status = 200,
            headers = {
                [ 'content-type'] = "application/json; charset=utf-8"
            },
            body = json.encode(result)
        }
    end

    httpd:route({ path = '/' }, static_handler)
    httpd:route({ path = '/static/css/graphiql.css' }, static_handler)
    httpd:route({ path = '/static/js/graphiql.js' }, static_handler)
    httpd:route({ path = '/graphql' }, api_handler)

    return httpd
end

return server
