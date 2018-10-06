local utils = require('graphql.utils')

local request_batch = {}

local function iterator_opts_tostring(iterator_opts)
    return ('%s,%s'):format(
        iterator_opts.iterator or iterator_opts[1] or 'EQ',
        iterator_opts.limit or '')
end

-- String uniquely identifies the key.
local function key_tostring(key)
    if type(key) ~= 'table' then
        return tostring(key)
    end

    assert(utils.is_array(key), 'compound key must be an array')

    if #key == 0 then
        return ''
    end

    -- We cannot use table.concat here, because it does not convert cdata to a
    -- string.
    local res = tostring(key[1])
    for i = 2, #key do
        res = (', %s%s'):format(res, tostring(key))
    end
    return res
end

--- List of strings, each uniquely identifies select request in the batch.
local function batch_select_ids(self, skip_function)
    local ids = {}

    local collection_name = self.collection_name
    local index_name = self.index_name or ''
    local iterator_opts_str = iterator_opts_tostring(self.iterator_opts)

    for _, key in ipairs(self.keys) do
        local key_str = key_tostring(key)
        local id = ('%s.%s.%s.%s'):format(collection_name, index_name, key_str,
            iterator_opts_str)
        if skip_function == nil or not skip_function(id) then
            table.insert(ids, id)
        end
    end

    return ids
end

--- String uniquely identifies the batch information except keys.
local function batch_bin(self)
    return ('%s.%s.%s'):format(
        self.collection_name,
        self.index_name or '',
        iterator_opts_tostring(self.iterator_opts))
end

--- Compare batches by bin.
local function batch_compare_bins(self, other)
    return self.collection_name == other.collection_name and
        self.index_name == other.index_name and
        utils.are_tables_same(self.iterator_opts, other.iterator_opts)
end

-- Add a key to the batch and update the keys hash.
local function batch_add_key(self, key)
    local key_str = key_tostring(key)
    if self.keys_hash[key_str] then
        return
    end
    self.keys_hash[key_str] = true
    table.insert(self.keys, key)
end

local request_batch_mt = {
    __index = {
        bin = batch_bin,
        select_ids = batch_select_ids,
        compare_bins = batch_compare_bins,
        add_key = batch_add_key,
    }
}

function request_batch.new(collection_name, index_name, keys, iterator_opts)
    local res = setmetatable({
        collection_name = collection_name,
        index_name = index_name,
        keys = {},
        iterator_opts = iterator_opts or {},
        keys_hash = {},
    }, request_batch_mt)

    for _, key in ipairs(keys) do
        res:add_key(key)
    end

    return res
end

function request_batch.from_prepared_resolve(prepared_resolve)
    assert(not prepared_resolve.is_calculated)
    local prepared_select = prepared_resolve.prepared_select
    local request_opts = prepared_select.request_opts

    local collection_name = prepared_select.collection_name
    local index_name = request_opts.index_name
    local keys = {request_opts.index_value or box.NULL}
    local iterator_opts = request_opts.iterator_opts

    return request_batch.new(collection_name, index_name, keys, iterator_opts)
end

return request_batch
