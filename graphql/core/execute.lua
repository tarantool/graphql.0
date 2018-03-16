local path = (...):gsub('%.[^%.]+$', '')
local types = require(path .. '.types')
local util = require(path .. '.util')
local introspection = require(path .. '.introspection')
local query_util = require(path .. '.query_util')

local function typeFromAST(node, schema)
  local innerType
  if node.kind == 'listType' then
    innerType = typeFromAST(node.type)
    return innerType and types.list(innerType)
  elseif node.kind == 'nonNullType' then
    innerType = typeFromAST(node.type)
    return innerType and types.nonNull(innerType)
  else
    assert(node.kind == 'namedType', 'Variable must be a named type')
    return schema:getType(node.name.value)
  end
end

local function getFieldResponseKey(field)
  return field.alias and field.alias.name.value or field.name.value
end

local function shouldIncludeNode(selection, context)
  if selection.directives then
    local function isDirectiveActive(key, _type)
      local directive = util.find(selection.directives, function(directive)
        return directive.name.value == key
      end)

      if not directive then return end

      local ifArgument = util.find(directive.arguments, function(argument)
        return argument.name.value == 'if'
      end)

      if not ifArgument then return end

      return util.coerceValue(ifArgument.value, _type.arguments['if'], context.variables)
    end

    if isDirectiveActive('skip', types.skip) then return false end
    if isDirectiveActive('include', types.include) == false then return false end
  end

  return true
end

local function doesFragmentApply(fragment, type, context)
  if not fragment.typeCondition then return true end

  local innerType = typeFromAST(fragment.typeCondition, context.schema)

  if innerType == type then
    return true
  elseif innerType.__type == 'Interface' then
    local implementors = context.schema:getImplementors(innerType.name)
    return implementors and implementors[type]
  elseif innerType.__type == 'Union' then
    return util.find(innerType.types, function(member)
      return member == type
    end)
  end
end

local function defaultResolver(object, arguments, info)
  return object[info.fieldASTs[1].name.value]
end

local evaluateSelections

--@todo resolveType optional comments
local function completeValue(fieldType, result, subSelections, context, resolvedType)
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

  error('Unknown type "' .. fieldTypeName .. '" for field "' .. field.name .. '"')
end

local function getFieldEntry(objectType, object, fields, context)
  local firstField = fields[1]
  local fieldName = firstField.name.value
  local responseKey = getFieldResponseKey(firstField)
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
    return supplied and util.coerceValue(supplied, argument, context.variables) or argument.defaultValue
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
  --@todo add comment
  local resolvedObject, resolvedType = (fieldType.resolve or defaultResolver)(object, arguments, info)
  local subSelections = query_util.mergeSelectionSets(fields)

  --@todo add comment
  return completeValue(fieldType.kind, resolvedObject, subSelections, context, resolvedType)
end

evaluateSelections = function(objectType, object, selections, context)
  local groupedFieldSet = query_util.collectFields(objectType, selections, {}, {}, context)

  return util.map(groupedFieldSet, function(fields)
    return getFieldEntry(objectType, object, fields, context)
  end)
end

return function(schema, tree, rootValue, variables, operationName)
  local context = query_util.buildContext(schema, tree, rootValue, variables, operationName)
  -- The field is passed to resolve function within info attribute.
  -- Can be used to store any data within one query.
  context.qcontext = {}
  local rootType = schema[context.operation.operation]

  if not rootType then
    error('Unsupported operation "' .. context.operation.operation .. '"')
  end

  return evaluateSelections(rootType, rootValue, context.operation.selectionSet.selections, context)
end
