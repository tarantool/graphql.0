local error_codes = {}

error_codes.TYPE_MISMATCH = 1
error_codes.WRONG_VALUE = 2

function error_codes.type_mismatch(message)
    return {
        message = message,
        extensions = {
            error_code = error_codes.TYPE_MISMATCH,
        }
    }
end

function error_codes.wrong_value(message)
    return {
        message = message,
        extensions = {
            error_code = error_codes.WRONG_VALUE,
        }
    }
end

return error_codes
