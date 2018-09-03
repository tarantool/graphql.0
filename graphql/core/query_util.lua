local path = (...):gsub('%.[^%.]+$', '')
local types = require(path .. '.types')
local util = require(path .. '.util')

local query_util = {}

function query_util.typeFromAST(node, schema)
  local innerType
  if node.kind == 'listType' then
    innerType = query_util.typeFromAST(node.type, schema)
    return innerType and types.list(innerType)
  elseif node.kind == 'nonNullType' then
    innerType = query_util.typeFromAST(node.type, schema)
    return innerType and types.nonNull(innerType)
  else
    assert(node.kind == 'namedType', 'Variable must be a named type')
    return schema:getType(node.name.value)
  end
end

function query_util.getFieldResponseKey(field)
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

  local innerType = query_util.typeFromAST(fragment.typeCondition, context.schema)

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

function query_util.collectFields(objectType, selections, visitedFragments, result, context)
  for _, selection in ipairs(selections) do
    if selection.kind == 'field' then
      if shouldIncludeNode(selection, context) then
        local name = query_util.getFieldResponseKey(selection)
        table.insert(result, {name = name, selection = selection})
      end
    elseif selection.kind == 'inlineFragment' then
      if shouldIncludeNode(selection, context) and doesFragmentApply(selection, objectType, context) then
        query_util.collectFields(objectType, selection.selectionSet.selections, visitedFragments, result, context)
      end
    elseif selection.kind == 'fragmentSpread' then
      local fragmentName = selection.name.value
      if shouldIncludeNode(selection, context) and not visitedFragments[fragmentName] then
        visitedFragments[fragmentName] = true
        local fragment = context.fragmentMap[fragmentName]
        if fragment and shouldIncludeNode(fragment, context) and doesFragmentApply(fragment, objectType, context) then
          query_util.collectFields(objectType, fragment.selectionSet.selections, visitedFragments, result, context)
        end
      end
    end
  end

  return result
end

function query_util.mergeSelectionSets(fields)
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

function query_util.getOperation(tree, operationName)
    local operation

    for _, definition in ipairs(tree.definitions) do
        if definition.kind == 'operation' then
            if not operationName and operation then
                error('Operation name must be specified if more than one operation exists.')
            end

            if not operationName or definition.name.value == operationName then
                operation = definition
            end
        end
    end

    if not operation then
        if operationName then
            error('Unknown operation "' .. operationName .. '"')
        else
            error('Must provide an operation')
        end
    end

    return operation
end

function query_util.getFragmentDefinitions(tree)
    local fragmentMap = {}

    for _, definition in ipairs(tree.definitions) do
        if definition.kind == 'fragmentDefinition' then
            fragmentMap[definition.name.value] = definition
        end
    end

    return fragmentMap
end

-- Extract variableTypes from the operation.
function query_util.getVariableTypes(schema, operation)
    local variableTypes = {}

    for _, definition in ipairs(operation.variableDefinitions or {}) do
        variableTypes[definition.variable.name.value] =
            query_util.typeFromAST(definition.type, schema)
    end

    return variableTypes
end

function query_util.buildContext(schema, tree, rootValue, variables, operationName)
    local operation = query_util.getOperation(tree, operationName)
    local fragmentMap = query_util.getFragmentDefinitions(tree)
    local variableTypes = query_util.getVariableTypes(schema, operation)
    return {
        schema = schema,
        rootValue = rootValue,
        variables = variables,
        operation = operation,
        fragmentMap = fragmentMap,
        variableTypes = variableTypes,
    }
end

return query_util
