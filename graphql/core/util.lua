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
  return s:gsub('^%s+', ''):gsub('%s$', ''):gsub('%s%s+', ' ')
end

function util.coerceValue(node, schemaType, variables)
  variables = variables or {}

  if schemaType.__type == 'NonNull' then
    return util.coerceValue(node, schemaType.ofType, variables)
  end

  if not node then
    return nil
  end

  if node.kind == 'variable' then
    return variables[node.name.value]
  end

  if schemaType.__type == 'List' then
    if node.kind ~= 'list' then
      error('Expected a list')
    end

    return util.map(node.values, function(value)
      return util.coerceValue(value, schemaType.ofType, variables)
    end)
  end

  local isInputObject = schemaType.__type == 'InputObject'
  local isInputMap = schemaType.__type == 'Scalar' and
    schemaType.subtype == 'InputMap'
  local isInputUnion = schemaType.__type == 'Scalar' and
    schemaType.subtype == 'InputUnion'

  if isInputObject or isInputMap then
    if node.kind ~= 'inputObject' then
      error('Expected an input object')
    end

    local inputObjectValue = {}
    for _, field in pairs(node.values) do
      if isInputObject and not schemaType.fields[field.name] then
        error('Unknown input object field "' .. field.name .. '"')
      end

      local child_type = isInputObject and schemaType.fields[field.name].kind or
        schemaType.values
      inputObjectValue[field.name] = util.coerceValue(field.value, child_type,
        variables)
    end
    return inputObjectValue
  end

  if schemaType.__type == 'Enum' then
    if node.kind ~= 'enum' then
      error('Expected enum value, got ' .. node.kind)
    end

    if not schemaType.values[node.value] then
      error('Invalid enum value "' .. node.value .. '"')
    end

    return node.value
  end

  if isInputUnion then
    local child_type = schemaType.resolveNodeType(node)
    return util.coerceValue(node, child_type, variables)
  end

  if schemaType.__type == 'Scalar' then
    if schemaType.parseLiteral(node) == nil then
      error('Could not coerce "' .. tostring(node.value) .. '" to "' .. schemaType.name .. '"')
    end

    return schemaType.parseLiteral(node)
  end
end

return util
