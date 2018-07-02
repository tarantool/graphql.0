local error_codes = {}

error_codes.TYPE_MISMATCH = 1
error_codes.WRONG_VALUE = 2
error_codes.TIMEOUT_EXCEEDED = 3
error_codes.FETCHED_OBJECTS_LIMIT_EXCEEDED = 4
error_codes.RESULTING_OBJECTS_LIMIT_EXCEEDED = 5

local function message_and_error_code_error(message, error_code)
    return {
        message = message,
        extensions = {
            error_code = error_code,
        }
    }
end

function error_codes.type_mismatch(message)
    local error_code = error_codes.TYPE_MISMATCH
    return message_and_error_code_error(message, error_code)
end

function error_codes.wrong_value(message)
    local error_code = error_codes.WRONG_VALUE
    return message_and_error_code_error(message, error_code)
end

function error_codes.timeout_exceeded(message)
    local error_code = error_codes.TIMEOUT_EXCEEDED
    return message_and_error_code_error(message, error_code)
end

function error_codes.fetched_objects_limit_exceeded(message)
    local error_code = error_codes.FETCHED_OBJECTS_LIMIT_EXCEEDED
    return message_and_error_code_error(message, error_code)
end

function error_codes.resulting_objects_limit_exceeded(message)
    local error_code = error_codes.RESULTING_OBJECTS_LIMIT_EXCEEDED
    return message_and_error_code_error(message, error_code)
end

return error_codes
