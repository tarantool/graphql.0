local path = (...):gsub('%.[^%.]+$', '')
local util = require(path .. '.util')

local types = {}

function types.nonNull(kind)
  assert(kind, 'Must provide a type')

  return {
    __type = 'NonNull',
    ofType = kind
  }
end

function types.list(kind)
  assert(kind, 'Must provide a type')

  return {
    __type = 'List',
    ofType = kind
  }
end

function types.nullable(kind)
    assert(type(kind) == 'table', 'kind must be a table, got ' .. type(kind))

    if kind.__type ~= 'NonNull' then return kind end

    assert(kind.ofType ~= nil, 'kind.ofType must not be nil')
    return types.nullable(kind.ofType)
end

function types.bare(kind)
    assert(type(kind) == 'table', 'kind must be a table, got ' .. type(kind))

    if kind.ofType == nil then return kind end

    assert(kind.ofType ~= nil, 'kind.ofType must not be nil')
    return types.bare(kind.ofType)
end

function types.scalar(config)
  assert(type(config.name) == 'string', 'type name must be provided as a string')
  assert(type(config.serialize) == 'function', 'serialize must be a function')
  if config.parseValue or config.parseLiteral then
    assert(
      type(config.parseValue) == 'function' and type(config.parseLiteral) == 'function',
      'must provide both parseValue and parseLiteral to scalar type'
    )
  end

  local instance = {
    __type = 'Scalar',
    name = config.name,
    description = config.description,
    serialize = config.serialize,
    parseValue = config.parseValue,
    parseLiteral = config.parseLiteral
  }

  instance.nonNull = types.nonNull(instance)

  return instance
end

function types.object(config)
  assert(type(config.name) == 'string', 'type name must be provided as a string')
  if config.isTypeOf then
    assert(type(config.isTypeOf) == 'function', 'must provide isTypeOf as a function')
  end

  local fields
  if type(config.fields) == 'function' then
    fields = util.compose(util.bind1(initFields, 'Object'), config.fields)
  else
    fields = initFields('Object', config.fields)
  end

  local instance = {
    __type = 'Object',
    name = config.name,
    description = config.description,
    isTypeOf = config.isTypeOf,
    fields = fields,
    interfaces = config.interfaces
  }

  instance.nonNull = types.nonNull(instance)

  return instance
end

function types.interface(config)
  assert(type(config.name) == 'string', 'type name must be provided as a string')
  assert(type(config.fields) == 'table', 'fields table must be provided')
  if config.resolveType then
    assert(type(config.resolveType) == 'function', 'must provide resolveType as a function')
  end

  local fields
  if type(config.fields) == 'function' then
    fields = util.compose(util.bind1(initFields, 'Interface'), config.fields)
  else
    fields = initFields('Interface', config.fields)
  end

  local instance = {
    __type = 'Interface',
    name = config.name,
    description = config.description,
    fields = fields,
    resolveType = config.resolveType
  }

  instance.nonNull = types.nonNull(instance)

  return instance
end

function initFields(kind, fields)
  assert(type(fields) == 'table', 'fields table must be provided')

  local result = {}

  for fieldName, field in pairs(fields) do
    field = field.__type and { kind = field } or field
    result[fieldName] = {
      name = fieldName,
      kind = field.kind,
      description = field.description,
      deprecationReason = field.deprecationReason,
      arguments = field.arguments or {},
      resolve = kind == 'Object' and field.resolve or nil
    }
  end

  return result
end

function types.enum(config)
  assert(type(config.name) == 'string', 'type name must be provided as a string')
  assert(type(config.values) == 'table', 'values table must be provided')

  local instance
  local values = {}

  for name, entry in pairs(config.values) do
    entry = type(entry) == 'table' and entry or { value = entry }

    values[name] = {
      name = name,
      description = entry.description,
      deprecationReason = entry.deprecationReason,
      value = entry.value
    }
  end

  instance = {
    __type = 'Enum',
    name = config.name,
    description = config.description,
    values = values,
    serialize = function(name)
      return instance.values[name] and instance.values[name].value or name
    end
  }

  instance.nonNull = types.nonNull(instance)

  return instance
end

function types.union(config)
  assert(type(config.name) == 'string', 'type name must be provided as a string')
  assert(type(config.types) == 'table', 'types table must be provided')
  if config.resolveType then
    assert(type(config.resolveType) == 'function', 'must provide resolveType as a function')
  end

  local instance = {
    __type = 'Union',
    name = config.name,
    types = config.types,
    resolveType = config.resolveType
  }

  instance.nonNull = types.nonNull(instance)

  return instance
end

types.map = types.scalar({
  name = 'Map',
  description = 'Map is a dictionary with string keys and values of ' ..
    'arbitrary but same among all values type',
  serialize = function(value) return value end,
  parseValue = function(value) return value end,
  parseLiteral = function(node)
    error('Literal parsing is implemented in util.coerceValue; ' ..
      'we should not go here')
  end,
})

function types.inputObject(config)
  assert(type(config.name) == 'string', 'type name must be provided as a string')

  local fields = {}
  for fieldName, field in pairs(config.fields) do
    field = field.__type and { kind = field } or field
    fields[fieldName] = {
      name = fieldName,
      kind = field.kind
    }
  end

  local instance = {
    __type = 'InputObject',
    name = config.name,
    description = config.description,
    fields = fields
  }

  return instance
end

function types.inputMap(config)
  local instance = {
    __type = 'InputMap',
    name = config.name,
    serialize = function(value) return value end,
    parseValue = function(value) return value end,
    parseLiteral = function(node)
      error('Literal parsing is implemented in util.coerceValue; ' ..
        'we should not go here')
    end,
    values = config.values,
  }

  instance.nonNull = types.nonNull(instance)

  return instance
end

function types.inputUnion(config)
  local instance = {
    __type = 'InputUnion',
    name = config.name,
    serialize = function(value) return value end,
    parseValue = function(value) return value end,
    parseLiteral = function(node)
      error('Literal parsing is implemented in util.coerceValue; ' ..
        'we should not go here')
    end,
    resolveNodeType = config.resolveNodeType,
  }

  instance.nonNull = types.nonNull(instance)

  return instance
end

local coerceInt = function(value)
  value = tonumber(value)

  if not value then return end

  if value == value and value < 2 ^ 32 and value >= -2 ^ 32 then
    return value < 0 and math.ceil(value) or math.floor(value)
  end
end

types.int = types.scalar({
  name = 'Int',
  description = "The `Int` scalar type represents non-fractional signed whole numeric values. Int can represent values between -(2^31) and 2^31 - 1. ",
  serialize = coerceInt,
  parseValue = coerceInt,
  parseLiteral = function(node)
    if node.kind == 'int' then
      return coerceInt(node.value)
    end
  end
})

types.long = types.scalar({
  name = 'Long',
  description = 'Long is non-bounded integral type',
  serialize = function(value) return tonumber(value) end,
  parseValue = function(value) return tonumber(value) end,
  parseLiteral = function(node)
   -- 'int' is name of the immediate value type
   if node.kind == 'int' then
     return tonumber(node.value)
   end
  end
})

types.float = types.scalar({
  name = 'Float',
  serialize = tonumber,
  parseValue = tonumber,
  parseLiteral = function(node)
    if node.kind == 'float' or node.kind == 'int' then
      return tonumber(node.value)
    end
  end
})

types.double = types.scalar({
  name = 'Double',
  serialize = tonumber,
  parseValue = tonumber,
  parseLiteral = function(node)
    -- 'float' and 'int' are names of immediate value types
    if node.kind == 'float' or node.kind == 'int' then
      return tonumber(node.value)
    end
  end
})

types.string = types.scalar({
  name = 'String',
  description = "The `String` scalar type represents textual data, represented as UTF-8 character sequences. The String type is most often used by GraphQL to represent free-form human-readable text.",
  serialize = tostring,
  parseValue = tostring,
  parseLiteral = function(node)
    if node.kind == 'string' then
      return node.value
    end
  end
})

local function toboolean(x)
  return (x and x ~= 'false') and true or false
end

types.boolean = types.scalar({
  name = 'Boolean',
  description = "The `Boolean` scalar type represents `true` or `false`.",
  serialize = toboolean,
  parseValue = toboolean,
  parseLiteral = function(node)
    if node.kind == 'boolean' then
      return toboolean(node.value)
    else
      return nil
    end
  end
})

types.id = types.scalar({
  name = 'ID',
  serialize = tostring,
  parseValue = tostring,
  parseLiteral = function(node)
    return node.kind == 'string' or node.kind == 'int' and node.value or nil
  end
})

function types.directive(config)
  assert(type(config.name) == 'string', 'type name must be provided as a string')

  local instance = {
    __type = 'Directive',
    name = config.name,
    description = config.description,
    arguments = config.arguments,
    onQuery = config.onQuery,
    onMutation = config.onMutation,
    onField = config.onField,
    onFragmentDefinition = config.onFragmentDefinition,
    onFragmentSpread = config.onFragmentSpread,
    onInlineFragment = config.onInlineFragment
  }

  return instance
end

types.include = types.directive({
  name = 'include',
  description = 'Directs the executor to include this field or fragment only when the `if` argument is true.',
  arguments = {
    ['if'] = { kind = types.boolean.nonNull, description = 'Included when true.'}
  },
  onField = true,
  onFragmentSpread = true,
  onInlineFragment = true
})

types.skip = types.directive({
  name = 'skip',
  description = 'Directs the executor to skip this field or fragment when the `if` argument is true.',
  arguments = {
    ['if'] = { kind = types.boolean.nonNull, description = 'Skipped when true.' }
  },
  onField = true,
  onFragmentSpread = true,
  onInlineFragment = true
})

return types
