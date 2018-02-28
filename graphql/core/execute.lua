local path = (...):gsub('%.[^%.]+$', '')
local types = require(path .. '.types')
local util = require(path .. '.util')
local introspection = require(path .. '.introspection')

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

local function mergeSelectionSets(fields)
  local selections = {}

  for i = 1, #fields do
    local selectionSet = fields[i].selectionSet
    if selectionSet then
      for j = 1, #selectionSet.selections do
        table.insert(selections, selectionSet.selections[j])
      end
    end
  end

  return selections
end

local function defaultResolver(object, arguments, info)
  --print('print from default resolver in execute 82 ')
  --print(object)
  --require('pl.pretty').dump(object)
  --require('pl.pretty').dump(object)
  --print('print from default resolver in 86')
  --require('pl.pretty').dump(info.fieldASTs[1].name.value)
  --require('pl.pretty').dump(object[info.fieldASTs[1].name.value])
  return object[info.fieldASTs[1].name.value]
end

local function buildContext(schema, tree, rootValue, variables, operationName)
  local context = {
    schema = schema,
    rootValue = rootValue,
    variables = variables,
    operation = nil,
    fragmentMap = {}
  }

  for _, definition in ipairs(tree.definitions) do
    if definition.kind == 'operation' then
      if not operationName and context.operation then
        error('Operation name must be specified if more than one operation exists.')
      end

      if not operationName or definition.name.value == operationName then
        context.operation = definition
      end
    elseif definition.kind == 'fragmentDefinition' then
      context.fragmentMap[definition.name.value] = definition
    end
  end

  if not context.operation then
    if operationName then
      error('Unknown operation "' .. operationName .. '"')
    else
      error('Must provide an operation')
    end
  end

  return context
end

local function collectFields(objectType, selections, visitedFragments, result, context)
  for _, selection in ipairs(selections) do
    if selection.kind == 'field' then
      if shouldIncludeNode(selection, context) then
        local name = getFieldResponseKey(selection)
        result[name] = result[name] or {}
        table.insert(result[name], selection)
      end
    elseif selection.kind == 'inlineFragment' then
      if shouldIncludeNode(selection, context) and doesFragmentApply(selection, objectType, context) then
        collectFields(objectType, selection.selectionSet.selections, visitedFragments, result, context)
      end
    elseif selection.kind == 'fragmentSpread' then
      local fragmentName = selection.name.value
      if shouldIncludeNode(selection, context) and not visitedFragments[fragmentName] then
        visitedFragments[fragmentName] = true
        local fragment = context.fragmentMap[fragmentName]
        if fragment and shouldIncludeNode(fragment, context) and doesFragmentApply(fragment, objectType, context) then
          collectFields(objectType, fragment.selectionSet.selections, visitedFragments, result, context)
        end
      end
    end
  end

  return result
end

local evaluateSelections

--- check if given object is flat and have only scalars
local function is_simple_object(object)
  assert(type(object) == 'table', 'object type must be table')

  for _, v in pairs(object) do
    if type(v) == 'table' or type(v) == 'function' then
      return false
    end
  end

  for _, v in ipairs(object) do
    if type(v) == 'table' or type(v) == 'function' then
      return false
    end
  end

  return true
end

local function completeValue(fieldType, result, subSelections, context)
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
      --print('print from List section in completeValue() 182 with i == ' .. i)
      --print('INNER TYPE')
      --require('pl.pretty').dump(innerType)
      --print('VALUE')
      --require('pl.pretty').dump(value)
      --
      --print("is simple object?")
      --print(is_simple_object(value))

      --values[i] = is_simple_object(value) and value or
      --  completeValue(innerType, value, subSelections, context)
      values[i] = completeValue(innerType, value, subSelections, context)
      --print('result after completeValue()')
      --require('pl.pretty').dump(value[i])

    end

    return next(values) and values or context.schema.__emptyList
  end

  if fieldTypeName == 'Scalar' or fieldTypeName == 'Enum' then
    return fieldType.serialize(result)
  end

  if fieldTypeName == 'Object' then
    -- можно добавить условие, что если объект простой
    -- и вложенностей нет
    -- то вернуть его как есть и не делать evaluateSelections
    local fields = evaluateSelections(fieldType, result, subSelections, context)
    return next(fields) and fields or context.schema.__emptyObject
  elseif fieldTypeName == 'Interface' or fieldTypeName == 'Union' then
    local objectType = fieldType.resolveType(result)
    return evaluateSelections(objectType, result, subSelections, context)
  end

  error('Unknown type "' .. fieldTypeName .. '" for field "' .. field.name .. '"')
end

local function getFieldEntry(objectType, object, fields, context)

  --print('print from 242 execute lua')

  local firstField = fields[1]
  local fieldName = firstField.name.value
  local responseKey = getFieldResponseKey(firstField)
  local fieldType = introspection.fieldMap[fieldName] or objectType.fields[fieldName]
  --
  --require('pl.pretty').dump(fieldName)
  --require('pl.pretty').dump(fieldType)


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
    variableValues = context.variables
  }

  local resolvedObject = (fieldType.resolve or defaultResolver)(object, arguments, info)
  local subSelections = mergeSelectionSets(fields)
  --print('print from 258 in execute')
  --require('pl.pretty').dump(resolvedObject)


  --print('resolvedObject')
  --require('pl.pretty').dump(objectType)

  local res = completeValue(fieldType.kind, resolvedObject, subSelections, context)
  --print('complete value result from 256 in execute.lua')
  --require('pl.pretty').dump(res)

  return res
end

evaluateSelections = function(objectType, object, selections, context)
  local groupedFieldSet = collectFields(objectType, selections, {}, {}, context)

  --print('print from 298, groupedFieldSet')
  --require('pl.pretty').dump(groupedFieldSet)
  --print('OBJECT')
  --require('pl.pretty').dump(object)
  --print('OBJECT TYPE')
  --require('pl.pretty').dump(objectType)
  --print('SELECTIONS')
  --require('pl.pretty').dump(selections)




  return util.map(groupedFieldSet, function(fields)
    return getFieldEntry(objectType, object, fields, context)
  end)
end

return function(schema, tree, rootValue, variables, operationName)


  local context = buildContext(schema, tree, rootValue, variables, operationName)
  local rootType = schema[context.operation.operation]



  if not rootType then
    error('Unsupported operation "' .. context.operation.operation .. '"')
  end



  return evaluateSelections(rootType, rootValue, context.operation.selectionSet.selections, context)
end
