#!/usr/bin/env tarantool

local fiber = require('fiber')
local log = require('log')

-- get instance name from filename (shard_2x2_1.lua => 1)
local INSTANCE_ID = arg[0]:gsub('^shard_[0-9]x[0-9]_([0-9]).lua$', '%1')
local INSTANCE_TYPE = arg[0]:gsub('^(shard_[0-9]x[0-9])_[0-9].lua$', '%1')
local SOCKET_DIR = require('fio').cwd()

local function instance_uri(instance_type, instance_id)
    return ('%s/%s_%s.sock'):format(SOCKET_DIR, instance_type, instance_id)
end

-- start console first
require('console').listen(os.getenv('ADMIN'))

local replication
if INSTANCE_TYPE == 'shard_2x2' then
    if INSTANCE_ID == '1' or INSTANCE_ID == '2' then
        replication = {
            instance_uri(INSTANCE_TYPE, '1'),
            instance_uri(INSTANCE_TYPE, '2'),
        }
    elseif INSTANCE_ID == '3' or INSTANCE_ID == '4' then
        replication = {
            instance_uri(INSTANCE_TYPE, '3'),
            instance_uri(INSTANCE_TYPE, '4'),
        }
    else
        error(('Unknown instance id "%s" for instance type "%s"'):format(
            tostring(INSTANCE_ID), tostring(INSTANCE_TYPE)))
    end
end

box.cfg({
    replication = replication,
    listen = instance_uri(INSTANCE_TYPE, INSTANCE_ID),
})

box.once('shard_init', function()
    box.schema.user.grant('guest', 'read,write,execute', 'universe')
end)

local function wait_for_replicas()
    local wait_lsn = box.info.lsn
    local id = box.info.id
    local in_progress = true
    local iter = 0
    while in_progress do
        in_progress = false
        for node_id, info in pairs(box.info.replication) do
            if node_id ~= id then
                if info.downstream ~= nil and
                        info.downstream.vclock ~= nil and
                        info.downstream.vclock[id] ~= nil then
                    local lsn = info.downstream.vclock[id]
                    in_progress = in_progress or lsn < wait_lsn
                else
                    in_progress = true
                end
            end
        end
        fiber.sleep(0.001)
        iter = iter + 1
        if iter % 1000 == 0 then
            log.info(("wait for lsn %d in vclock['%s'] for replicas (iter %d)")
                :format(wait_lsn, id, iter))
        end
    end
end

_G.wait_for_replicas = wait_for_replicas
