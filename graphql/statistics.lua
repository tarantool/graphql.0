--- Count various statistics about a query.

local utils = require('graphql.utils')
local error_codes = require('graphql.error_codes')

local check = utils.check
local e = error_codes

local statistics = {}

--- Count fetch event.
local function objects_fetched(self, info)
    check(self, 'self', 'table')
    check(info, 'info', 'table')
    local fetches_cnt = info.fetches_cnt
    local fetched_objects_cnt = info.fetched_objects_cnt
    local full_scan_cnt = info.full_scan_cnt
    local index_lookup_cnt = info.index_lookup_cnt
    check(fetches_cnt, 'fetches_cnt', 'number')
    check(fetched_objects_cnt, 'fetched_objects_cnt', 'number')
    check(full_scan_cnt, 'full_scan_cnt', 'number')
    check(index_lookup_cnt, 'index_lookup_cnt', 'number')

    -- count fetches and fetched objects
    self.fetches_cnt = self.fetches_cnt + fetches_cnt
    self.fetched_object_cnt = self.fetched_object_cnt + fetched_objects_cnt

    -- count full scan and index select request
    self.full_scan_cnt = self.full_scan_cnt + full_scan_cnt
    self.index_lookup_cnt = self.index_lookup_cnt + index_lookup_cnt

    if self.limits.fetched_object_cnt_max == nil then
        return
    end
    if self.fetched_object_cnt > self.limits.fetched_object_cnt_max then
        error(e.fetched_objects_limit_exceeded(
            ('fetched objects count (%d) exceeds fetched_object_cnt_max ' ..
            'limit (%d)'):format(self.fetched_object_cnt,
            self.limits.fetched_object_cnt_max)))
    end
end

--- Count retire event.
local function objects_retired(self, info)
    check(self, 'self', 'table')
    check(info, 'info', 'table')
    local retired_objects_cnt = info.retired_objects_cnt
    check(retired_objects_cnt, 'retired_objects_cnt', 'number')

    self.resulting_object_cnt = self.resulting_object_cnt + retired_objects_cnt

    if self.limits.resulting_object_cnt_max == nil then
        return
    end
    if self.resulting_object_cnt > self.limits.resulting_object_cnt_max then
        error(e.resulting_objects_limit_exceeded(
            ('resulting objects count (%d) exceeds resulting_object_cnt_max ' ..
            'limit (%d)'):format(self.resulting_object_cnt,
            self.limits.resulting_object_cnt_max)))
    end
end

--- Count cache hit / miss event.
local function cache_lookup(self, info)
    check(self, 'self', 'table')
    check(info, 'info', 'table')
    local cache_hits_cnt = info.cache_hits_cnt
    local cache_hit_objects_cnt = info.cache_hit_objects_cnt
    check(cache_hits_cnt, 'cache_hits_cnt', 'number')
    check(cache_hit_objects_cnt, 'cache_hit_objects_cnt', 'number')

    self.cache_hits_cnt = self.cache_hits_cnt + cache_hits_cnt
    self.cache_hit_objects_cnt = self.cache_hit_objects_cnt +
        cache_hit_objects_cnt
end

function statistics.new(opts)
    local opts = opts or {}
    local resulting_object_cnt_max = opts.resulting_object_cnt_max
    local fetched_object_cnt_max = opts.fetched_object_cnt_max

    return setmetatable({
        resulting_object_cnt = 0,          -- retire
        fetches_cnt = 0,                   -- fetch
        fetched_object_cnt = 0,            -- fetch
        full_scan_cnt = 0,                 -- fetch
        index_lookup_cnt = 0,              -- fetch
        cache_hits_cnt = 0,                -- cache lookup
        cache_hit_objects_cnt = 0,         -- cache lookup
        limits = {
            resulting_object_cnt_max = resulting_object_cnt_max, -- retire limit
            fetched_object_cnt_max = fetched_object_cnt_max,     -- fetch limit
        }
    }, {
        __index = {
            objects_fetched = objects_fetched,
            objects_retired = objects_retired,
            cache_lookup = cache_lookup,
        }
    })
end

return statistics
