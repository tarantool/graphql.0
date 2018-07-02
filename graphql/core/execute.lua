local path = (...):gsub('%.[^%.]+$', '')
local util = require(path .. '.util')
local introspection = require(path .. '.introspection')
local query_util = require(path .. '.query_util')
local validate_variables = require(path .. '.validate_variables')

local function defaultResolver(object, arguments, info)
  return object[info.fieldASTs[1].name.value]
end

local evaluateSelections

-- @tparam[opt] table opts the following options:
--
-- * fieldName (string; optional)
--
-- * resolvedType (table; optional) resolvedType a type to be used instead of
--   one returned by `fieldType.resolveType(result)` in case when the
--   `fieldType` is Interface or Union; that is needed to increase flexibility
--   of an union type resolving (e.g. resolving by a parent object instead of a
--   current object) via returning it from the `fieldType.resolve` function,
--   which called before `resolvedType` and may need to determine the type
--   itself for its needs
local function completeValue(fieldType, result, subSelections, context, opts)
  local opts = opts or {}
  local resolvedType = opts.resolvedType
  local fieldName = opts.fieldName or '???'
  local fieldTypeName = fieldType.__type

  if fieldTypeName == 'NonNull' then
    local innerType = fieldType.ofType
    local completedResult = completeValue(innerType, result, subSelections, context)

    if completedResult == nil then
      error('No value provided for non-null ' .. (innerType.name or innerType.__type))
    end

    return completedResult
  end

  if result == nil then
    return nil
  end

  if fieldTypeName == 'List' then
    local innerType = fieldType.ofType

    if type(result) ~= 'table' then
      error('Expected a table for ' .. innerType.name .. ' list')
    end

    local values = {}
    for i, value in ipairs(result) do
      values[i] = completeValue(innerType, value, subSelections, context)
    end

    return next(values) and values or context.schema.__emptyList
  end

  if fieldTypeName == 'Scalar' or fieldTypeName == 'Enum' then
    return fieldType.serialize(result)
  end

  if fieldTypeName == 'Object' then
    local fields = evaluateSelections(fieldType, result, subSelections, context)
    return next(fields) and fields or context.schema.__emptyObject
  elseif fieldTypeName == 'Interface' or fieldTypeName == 'Union' then
    local objectType = resolvedType or fieldType.resolveType(result)
    while objectType.__type == 'NonNull' do
      objectType = objectType.ofType
    end

    return evaluateSelections(objectType, result, subSelections, context)
  end

  error('Unknown type "' .. fieldTypeName .. '" for field "' .. fieldName .. '"')
end

local function getFieldEntry(objectType, object, fields, context)
  local firstField = fields[1]
  local fieldName = firstField.name.value
  -- local responseKey = query_util.getFieldResponseKey(firstField)
  local fieldType = introspection.fieldMap[fieldName] or objectType.fields[fieldName]

  if fieldType == nil then
    return nil
  end

  local argumentMap = {}
  for _, argument in ipairs(firstField.arguments or {}) do
    argumentMap[argument.name.value] = argument
  end

  local arguments = util.map(fieldType.arguments or {}, function(argument, name)
    local supplied = argumentMap[name] and argumentMap[name].value
    supplied = util.coerceValue(supplied, argument, context.variables,
      {strict_non_null = true})
    if supplied ~= nil then
        return supplied
    else
        return argument.defaultValue
    end
  end)

  local info = {
    fieldName = fieldName,
    fieldASTs = fields,
    returnType = fieldType.kind,
    parentType = objectType,
    schema = context.schema,
    fragments = context.fragmentMap,
    rootValue = context.rootValue,
    operation = context.operation,
    variableValues = context.variables,
    qcontext = context.qcontext
  }

  -- resolvedType is optional return value
  local resolvedObject, resolvedType = (fieldType.resolve or defaultResolver)(object, arguments, info)
  local subSelections = query_util.mergeSelectionSets(fields)

  return completeValue(fieldType.kind, resolvedObject, subSelections, context,
    {resolvedType = resolvedType})
end

evaluateSelections = function(objectType, object, selections, context)
  local result = {}
  local fields = query_util.collectFields(objectType, selections, {}, {}, context)
  for _, field in ipairs(fields) do
    assert(result[field.name] == nil,
      'two selections into the one field: ' .. field.name)
    result[field.name] = getFieldEntry(objectType, object, {field.selection},
      context)
  end
  return result
end

return function(schema, tree, rootValue, variables, operationName, opts)
  local opts = opts or {}
  local context = query_util.buildContext(schema, tree, rootValue, variables, operationName)
  -- The field is passed to resolve function within info attribute.
  -- Can be used to store any data within one query.
  context.qcontext = opts.qcontext or {}
  local rootType = schema[context.operation.operation]

  if not rootType then
    error('Unsupported operation "' .. context.operation.operation .. '"')
  end

  validate_variables.validate_variables(context)

  return evaluateSelections(rootType, rootValue, context.operation.selectionSet.selections, context)
end
