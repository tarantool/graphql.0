#!/usr/bin/env tarantool

local stats = require('luacov.stats')
local runner = require('luacov.runner')

local accumulator = {}
local output_statsfile = 'luacov.stats.out'

for _, statsfile in ipairs(arg) do
    print(('Processing %s'):format(statsfile))
    local new_data = stats.load(statsfile)

    for name, item in pairs(new_data) do
       if accumulator[name] then
          runner.update_stats(accumulator[name], item)
       else
          accumulator[name] = item
       end
    end
end

stats.save(output_statsfile, accumulator)
