local types = require('graphql.core.types')
local graphql_utils = require('graphql.utils')
local graphql_error_codes = require('graphql.error_codes')

local check = graphql_utils.check
local e = graphql_error_codes

local validate_variables = {}

-- Traverse type more or less likewise util.coerceValue do.
local function checkVariableValue(variableName, value, variableType)
  check(variableName, 'variableName', 'string')
  check(variableType, 'variableType', 'table')

  local isNonNull = variableType.__type == 'NonNull'

  if isNonNull then
    variableType = types.nullable(variableType)
    if value == nil then
      error(e.wrong_value(('Variable "%s" expected to be non-null'):format(
        variableName)))
    end
  end

  local isList = variableType.__type == 'List'
  local isScalar = variableType.__type == 'Scalar'
  local isInputObject = variableType.__type == 'InputObject'
  local isInputMap = isScalar and variableType.subtype == 'InputMap'
  local isInputUnion = isScalar and variableType.subtype == 'InputUnion'

  -- Nullable variable type + null value case: value can be nil only when
  -- isNonNull is false.
  if value == nil then return end

  if isList then
    if type(value) ~= 'table' then
      error(e.wrong_value(('Variable "%s" for a List must be a Lua ' ..
        'table, got %s'):format(variableName, type(value))))
    end
    if not graphql_utils.is_array(value) then
      error(e.wrong_value(('Variable "%s" for a List must be an array, ' ..
        'got map'):format(variableName)))
    end
    assert(variableType.ofType ~= nil, 'variableType.ofType must not be nil')
    for i, item in ipairs(value) do
      local itemName = variableName .. '[' .. tostring(i) .. ']'
      checkVariableValue(itemName, item, variableType.ofType)
    end
    return
  end

  if isInputObject then
    if type(value) ~= 'table' then
      error(e.wrong_value(('Variable "%s" for the InputObject "%s" must ' ..
        'be a Lua table, got %s'):format(variableName, variableType.name,
        type(value))))
    end

    -- check all fields: as from value as well as from schema
    local fieldNameSet = {}
    for fieldName, _ in pairs(value) do
        fieldNameSet[fieldName] = true
    end
    for fieldName, _ in pairs(variableType.fields) do
        fieldNameSet[fieldName] = true
    end

    for fieldName, _ in pairs(fieldNameSet) do
      local fieldValue = value[fieldName]
      if type(fieldName) ~= 'string' then
        error(e.wrong_value(('Field key of the variable "%s" for the ' ..
          'InputObject "%s" must be a string, got %s'):format(variableName,
          variableType.name, type(fieldName))))
      end
      if type(variableType.fields[fieldName]) == 'nil' then
        error(e.wrong_value(('Unknown field "%s" of the variable "%s" ' ..
          'for the InputObject "%s"'):format(fieldName, variableName,
          variableType.name)))
      end

      local childType = variableType.fields[fieldName].kind
      local childName = variableName .. '.' .. fieldName
      checkVariableValue(childName, fieldValue, childType)
    end

    return
  end

  if isInputMap then
    if type(value) ~= 'table' then
      error(e.wrong_value(('Variable "%s" for the InputMap "%s" must be a ' ..
        'Lua table, got %s'):format(variableName, variableType.name,
        type(value))))
    end

    for fieldName, fieldValue in pairs(value) do
      if type(fieldName) ~= 'string' then
        error(e.wrong_value(('Field key of the variable "%s" for the ' ..
          'InputMap "%s" must be a string, got %s'):format(variableName,
          variableType.name, type(fieldName))))
      end
      local childType = variableType.values
      local childName = variableName .. '.' .. fieldName
      checkVariableValue(childName, fieldValue, childType)
    end

    return
  end

  -- XXX: Enum

  if isInputUnion then
    local childType = variableType.resolveType(value)
    checkVariableValue(variableName, value, childType)
    return
  end

  if isScalar then
    check(variableType.isValueOfTheType, 'isValueOfTheType', 'function')
    if not variableType.isValueOfTheType(value) then
      error(e.wrong_value(('Wrong variable "%s" for the Scalar "%s"'):format(
        variableName, variableType.name)))
    end
    return
  end

  error(('Unknown type of the variable "%s"'):format(variableName))
end

function validate_variables.validate_variables(context)
  -- check that all variable values have corresponding variable declaration
  for variableName, _ in pairs(context.variables or {}) do
    if context.variableTypes[variableName] == nil then
      error(e.wrong_value(('There is no declaration for the variable "%s"')
        :format(variableName)))
    end
  end

  -- check that variable values have correct type
  for variableName, variableType in pairs(context.variableTypes) do
    local value = (context.variables or {})[variableName]
    checkVariableValue(variableName, value, variableType)
  end
end

return validate_variables
