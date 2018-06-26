local types = require('graphql.core.types')
local graphql_utils = require('graphql.utils')

local check = graphql_utils.check

local validate_variables = {}

-- Traverse type more or less likewise util.coerceValue do.
local function checkVariableValue(variableName, value, variableType)
  check(variableName, 'variableName', 'string')
  check(variableType, 'variableType', 'table')

  local isNonNull = variableType.__type == 'NonNull'

  if isNonNull then
    variableType = types.nullable(variableType)
    assert(value ~= nil,
      ('Variable "%s" expected to be non-null'):format(variableName))
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
    assert(type(value) == 'table',
      ('Variable "%s" for a List must be a Lua table, got %s')
      :format(variableName, type(value)))
    assert(graphql_utils.is_array(value),
      ('Variable "%s" for a List must be an array, got map')
      :format(variableName))
    assert(variableType.ofType ~= nil, 'variableType.ofType must not be nil')
    for i, item in ipairs(value) do
      local itemName = variableName .. '[' .. tostring(i) .. ']'
      checkVariableValue(itemName, item, variableType.ofType)
    end
    return
  end

  if isInputObject then
    assert(type(value) == 'table',
      ('Variable "%s" for the InputObject "%s" must be a Lua table, ' ..
      'got %s'):format(variableName, variableType.name, type(value)))

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
      assert(type(fieldName) == 'string',
        ('Field key of the variable "%s" for the InputObject "%s" ' ..
        'must be a string, got %s'):format(variableName, variableType.name,
        type(fieldName)))
      assert(type(variableType.fields[fieldName]) ~= 'nil',
        ('Unknown field "%s" of the variable "%s" for the ' ..
        'InputObject "%s"'):format(fieldName, variableName, variableType.name))

      local childType = variableType.fields[fieldName].kind
      local childName = variableName .. '.' .. fieldName
      checkVariableValue(childName, fieldValue, childType)
    end

    return
  end

  if isInputMap then
    assert(type(value) == 'table',
      ('Variable "%s" for the InputMap "%s" must be a Lua table, got %s')
      :format(variableName, variableType.name, type(value)))

    for fieldName, fieldValue in pairs(value) do
      assert(type(fieldName) == 'string',
        ('Field key of the variable "%s" for the InputMap "%s" must be a ' ..
        'string, got %s'):format(variableName, variableType.name,
        type(fieldName)))
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
    assert(variableType.isValueOfTheType(value),
      ('Wrong variable "%s" for the Scalar "%s"'):format(
      variableName, variableType.name))
    return
  end

  error(('Unknown type of the variable "%s"'):format(variableName))
end

function validate_variables.validate_variables(context)
  -- check that all variable values have corresponding variable declaration
  for variableName, _ in pairs(context.variables or {}) do
    assert(context.variableTypes[variableName] ~= nil,
      ('There is no declaration for the variable "%s"'):format(variableName))
  end

  -- check that variable values have correct type
  for variableName, variableType in pairs(context.variableTypes) do
    local value = (context.variables or {})[variableName]
    checkVariableValue(variableName, value, variableType)
  end
end

return validate_variables
