local yaml = require('yaml')
local graphql_error_codes = require('graphql.error_codes')

local e = graphql_error_codes

local util = {}

function util.map(t, fn)
  local res = {}
  for k, v in pairs(t) do res[k] = fn(v, k) end
  return res
end

function util.find(t, fn)
  local res = {}
  for k, v in pairs(t) do
    if fn(v, k) then return v end
  end
end

function util.filter(t, fn)
  local res = {}
  for k,v in pairs(t) do
    if fn(v) then
      table.insert(res, v)
    end
  end
  return res
end

function util.values(t)
  local res = {}
  for _, value in pairs(t) do
    table.insert(res, value)
  end
  return res
end

function util.compose(f, g)
  return function(...) return f(g(...)) end
end

function util.bind1(func, x)
  return function(y)
    return func(x, y)
  end
end

function util.trim(s)
  return s:gsub('^%s+', ''):gsub('%s+$', ''):gsub('%s%s+', ' ')
end

function util.getTypeName(t)
  if t.name ~= nil then
    if t.name == 'Scalar' and t.subtype == 'InputMap' then
      return ('InputMap(%s)'):format(util.getTypeName(t.values))
    elseif t.name == 'Scalar' and t.subtype == 'InputUnion' then
      local typeNames = {}
      for _, child in ipairs(t.types) do
        table.insert(typeNames, util.getTypeName(child))
      end
      return ('InputUnion(%s)'):format(table.concat(typeNames, ','))
    end
    return t.name
  elseif t.__type == 'NonNull' then
    return ('NonNull(%s)'):format(util.getTypeName(t.ofType))
  elseif t.__type == 'List' then
    return ('List(%s)'):format(util.getTypeName(t.ofType))
  end

  local orig_encode_use_tostring = yaml.cfg.encode_use_tostring
  local err = ('Internal error: unknown type:\n%s'):format(yaml.encode(t))
  yaml.cfg({encode_use_tostring = orig_encode_use_tostring})
  error(err)
end

function util.coerceValue(node, schemaType, variables, opts)
  local variables = variables or {}
  local opts = opts or {}
  local strict_non_null = opts.strict_non_null or false

  if schemaType.__type == 'NonNull' then
    local res = util.coerceValue(node, schemaType.ofType, variables, opts)
    if strict_non_null and res == nil then
      error(e.wrong_value(('Expected non-null for "%s", got null'):format(
        util.getTypeName(schemaType))))
    end
    return res
  end

  if not node then
    return nil
  end

  -- handle precompiled values
  if node.compiled ~= nil then
    return node.compiled
  end

  if node.kind == 'variable' then
    return variables[node.name.value]
  end

  if schemaType.__type == 'List' then
    if node.kind ~= 'list' then
      error(e.wrong_value('Expected a list'))
    end

    return util.map(node.values, function(value)
      return util.coerceValue(value, schemaType.ofType, variables, opts)
    end)
  end

  local isInputObject = schemaType.__type == 'InputObject'
  local isInputMap = schemaType.__type == 'Scalar' and
    schemaType.subtype == 'InputMap'
  local isInputUnion = schemaType.__type == 'Scalar' and
    schemaType.subtype == 'InputUnion'

  if isInputObject then
    if node.kind ~= 'inputObject' then
      error(e.wrong_value('Expected an input object'))
    end

    -- check all fields: as from value as well as from schema
    local fieldNameSet = {}
    local fieldValues = {}
    for _, field in ipairs(node.values) do
        fieldNameSet[field.name] = true
        fieldValues[field.name] = field.value
    end
    for fieldName, _ in pairs(schemaType.fields) do
        fieldNameSet[fieldName] = true
    end

    local inputObjectValue = {}
    for fieldName, _ in pairs(fieldNameSet) do
      if not schemaType.fields[fieldName] then
        error(e.wrong_value(('Unknown input object field "%s"'):format(
          fieldName)))
      end

      local childValue = fieldValues[fieldName]
      local childType = schemaType.fields[fieldName].kind
      inputObjectValue[fieldName] = util.coerceValue(childValue, childType,
        variables, opts)
    end

    return inputObjectValue
  end

  if isInputMap then
    if node.kind ~= 'inputObject' then
      error(e.wrong_value('Expected an input object'))
    end

    local inputMapValue = {}
    for _, field in pairs(node.values) do
      local childType = schemaType.values
      inputMapValue[field.name] = util.coerceValue(field.value, childType,
        variables, opts)
    end
    return inputMapValue
  end

  if schemaType.__type == 'Enum' then
    if node.kind ~= 'enum' then
      error(e.wrong_value('Expected enum value, got %s'):format(node.kind))
    end

    if not schemaType.values[node.value] then
      error(e.wrong_value('Invalid enum value "%s"'):format(node.value))
    end

    return node.value
  end

  if isInputUnion then
    local child_type = schemaType.resolveNodeType(node)
    return util.coerceValue(node, child_type, variables, opts)
  end

  if schemaType.__type == 'Scalar' then
    if schemaType.parseLiteral(node) == nil then
      error(e.wrong_value(('Could not coerce "%s" to "%s"'):format(
        tostring(node.value), schemaType.name)))
    end

    return schemaType.parseLiteral(node)
  end
end

return util
