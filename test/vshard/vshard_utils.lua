local fio = require('fio')

-- require in-repo version of graphql/ sources despite current working directory
package.path = fio.abspath(debug.getinfo(1).source:match("@?(.*/)")
    :gsub('/./', '/'):gsub('/+$', '')) .. '/../../?.lua' .. ';' .. package.path

local id_to_uuid = {
    [1] = '8a274925-a26d-47fc-9e1b-af88ce939412',
    [2] = '1e02ae8a-afc0-4e91-ba34-843a356b8ed7',
    [3] = '48f49e1b-e9e1-45ef-9ffe-1cb85493c87d',
    [4] = 'b079d138-abf4-4862-a66a-486dcfc6ead2',
}

local id_to_replicaset_uuid = {
    [1] = 'cbf06940-0790-498b-948d-042b62cf3d29',
    [2] = 'ac522f65-aa94-4134-9f64-51ee384f1a54',
    [3] = '1e01a38d-4d7b-433f-a0da-892c478c7448',
    [4] = 'd7d37ad5-f23f-4204-9895-d05f2cc1fd72',
}


local SOCKET_DIR = require('fio').cwd()
local function instance_uri(instance_id)
    return ('%s/shard%d.sock'):format(SOCKET_DIR, instance_id)
end

local cfg_sharding = {}
for i = 1, 4 do
    cfg_sharding[id_to_replicaset_uuid[i]] = {
        replicas = {
            [id_to_uuid[i]] = {
                uri = 'guest@' .. instance_uri(i),
                name = 'shard' .. tostring(i),
                master = true,
            },
        }
    }
end

local cfg = {
    sharding = cfg_sharding,
}

local function init_vshard()
    local vutils = require('test.vshard.vshard_utils')
    local vshard = require('vshard')
    rawset(_G, 'vshard', vshard)
    assert(type(box.cfg) == 'table',
        'box.cfg have to be called in advance')
    vshard.storage.cfg(vutils.cfg, box.cfg.instance_uuid)
end

local nb = require('net.box')
local function cluster_eval(func, func_args)
    local func_script = string.dump(func)
    for _, rs in pairs(cfg.sharding) do
        for _, r in pairs(rs.replicas) do
            local c = nb.connect(r.uri, {wait_connected=true,
                connect_timeout=5})
            local ok, err = pcall(c.eval, c, func_script, func_args)
            assert(ok, tostring(err))
        end
    end
end

local function cluster_setup()
    cluster_eval(init_vshard, nil)
end

local lhash = require('vshard.hash')
local BUCKET_CNT = 3000
local function get_bucket_id(...)
    local key = {...}
    return lhash.key_hash(key) % BUCKET_CNT + 1
end

-- Add extra bucket_id field to a meta.
-- @param[return] meta table with schemas, indexes...
local function patch_non_vshard_meta(meta)
    for _, schema in pairs(meta.schemas) do
        assert(schema.type == 'record')
        table.insert(schema.fields, 1, {name = 'bucket_id', type = 'int'})
    end
    for _, index in pairs(meta.indexes) do
        index.bucket_id = {
            service_fields = {},
            fields = {'bucket_id'},
            index_type = 'tree',
            unique = false,
            primary = false,
        }
    end
    local vshard = {}
    local function find_pk(coll_indexes)
        for _, index in pairs(coll_indexes) do
            if index.primary then
                return index
            end
        end
        error('PK index is required')
    end
    for cname, collection in pairs(meta.collections) do
        vshard[cname] = {
            key_fields = find_pk(meta.indexes[cname]).fields,
            get_bucket_id = get_bucket_id,
            bucket_id_field = 'bucket_id',
            bucket_local_connections = {}
        }
    end
    meta.vshard = vshard
end

-- Get positions of bucket_id fields in a tuple by space_name.
local function get_bucket_id_positions(meta)
    local positions = {}
    for cname, collection in pairs(meta.collections) do
        positions[cname] = 1 + #meta.service_fields[collection.schema_name]
    end
    return positions
end

-- bucket_id_indexes = <{ <space_name> = <bucket_id_tuple_position> } }>
local function create_bucket_id_indexes(positions)
    for space_name, position in pairs(positions) do
        box.space[space_name]:create_index('bucket_id', {
            type = 'tree',
            parts = { position, 'unsigned' },
            unique = false
        })
    end
end

return {
    cfg = cfg,
    id_to_replicaset_uuid = id_to_replicaset_uuid,
    id_to_uuid = id_to_uuid,
    cluster_setup = cluster_setup,
    cluster_eval = cluster_eval,
    patch_non_vshard_meta = patch_non_vshard_meta,
    create_bucket_id_indexes = create_bucket_id_indexes,
    get_bucket_id_positions = get_bucket_id_positions,
}
