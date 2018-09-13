local id_to_uuid = {
    [1] = '8a274925-a26d-47fc-9e1b-af88ce939412',
    [2] = '1e02ae8a-afc0-4e91-ba34-843a356b8ed7',
    [3] = '48f49e1b-e9e1-45ef-9ffe-1cb85493c87d',
    [4] = 'b079d138-abf4-4862-a66a-486dcfc6ead2',
}

local replicasets = {
    [1] = 'cbf06940-0790-498b-948d-042b62cf3d29',
    [2] = 'ac522f65-aa94-4134-9f64-51ee384f1a54',
    [3] = '1e01a38d-4d7b-433f-a0da-892c478c7448',
    [4] = 'd7d37ad5-f23f-4204-9895-d05f2cc1fd72',
}

local id_to_replicaset_uuid = replicasets

local SOCKET_DIR = require('fio').cwd()
local function instance_uri(instance_id)
    return ('%s/shard%d.sock'):format(SOCKET_DIR, instance_id)
end

local cfg_sharding = {}
for i = 1, 4 do
    cfg_sharding[replicasets[i]] = {
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

return {
    id_to_uuid = id_to_uuid,
    id_to_replicaset_uuid = id_to_replicaset_uuid,
    cfg = cfg,
}
