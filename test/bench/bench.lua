-- requires
-- --------

local fio = require('fio')

-- require in-repo version of graphql/ sources despite current working directory
package.path = fio.abspath(debug.getinfo(1).source:match("@?(.*/)")
    :gsub('/./', '/'):gsub('/+$', '')) .. '/../../?.lua' .. ';' .. package.path

local tap = require('tap')
local yaml = require('yaml')
local clock = require('clock')
local fiber = require('fiber')
local digest = require('digest')
local multirunner = require('test.common.lua.multirunner')
local graphql = require('graphql')
local utils = require('graphql.utils')
local test_utils = require('test.utils')
local test_run = utils.optional_require('test_run')
test_run = test_run and test_run.new()

-- constants
-- ---------

local SCRIPT_DIR = fio.abspath(debug.getinfo(1).source:match("@?(.*/)")
    :gsub('/./', '/'):gsub('/+$', ''))

-- module
-- ------

local bench = {}

local function workload(shard, bench_prepare, bench_iter, opts)
    local iterations = opts.iterations
    local exp_checksum = opts.checksum
    local conf_type = opts.conf_type

    local state = {}
    state.shard = shard

    bench_prepare(state)

    local test = tap.test('workload')
    test:plan(1)

    local start_time = clock.monotonic64()

    local checksum = digest.crc32.new()

    -- first iteration; print result and update checksum
    local result = bench_iter(state)
    local result_str = yaml.encode(result)
    checksum:update(result_str .. '1')

    -- the rest iterations; just update checksum
    for i = 2, iterations do
        local result = bench_iter(state)
        local result_str = yaml.encode(result)
        checksum:update(result_str .. tostring(i))
        if i % 100 == 0 then
            fiber.yield()
        end
    end

    local end_time = clock.monotonic64()

    local checksum = checksum:result()
    local tap_extra = {
        result = result,
        checksum = checksum,
        conf_type = conf_type,
    }

    if exp_checksum == nil then
        -- report user the result to check and the checksum to fill in the test
        local msg = 'check results below and fill the test with checksum below'
        test:ok(false, msg, tap_extra)
    else
        test:is(checksum, exp_checksum, 'checksum', tap_extra)
    end

    local duration = tonumber(end_time - start_time) / 1000^3
    local latency_avg = duration / iterations
    local rps_avg = iterations / duration

    return {
        ok = checksum == exp_checksum,
        duration_successive = duration,
        latency_successive_avg = latency_avg,
        rps_successive_avg = rps_avg,
    }
end

local function write_result(test_name, conf_name, bench_result, to_file)
    local result_name = ('%s.%s'):format(test_name, conf_name)
    local result_suffix = os.getenv('RESULT_SUFFIX')
    if result_suffix ~= nil and result_suffix ~= '' then
        result_name = ('%s.%s'):format(result_name, result_suffix)
    end

    local result = ''
    result = result .. ('%s.duration_successive: %f\n'):format(
        result_name, bench_result.duration_successive)
    result = result .. ('%s.latency_successive_avg: %f\n'):format(
        result_name, bench_result.latency_successive_avg)
    result = result .. ('%s.rps_successive_avg: %f\n'):format(
        result_name, bench_result.rps_successive_avg)

    if not to_file then
        print(result)
        return
    end

    local timestamp = os.date('%Y%m%dT%H%M%S')
    local file_name = ('bench.%s.%s.result.txt'):format(
        result_name, timestamp)
    local file_path = fio.abspath(fio.pathjoin(SCRIPT_DIR, '../..', file_name))

    local open_flags = {'O_WRONLY', 'O_CREAT', 'O_TRUNC'}
    local fh, err = fio.open(file_path, open_flags, tonumber('644', 8))
    assert(fh ~= nil, ('open("%s", ...) error: %s'):format(file_path,
        tostring(err)))
    fh:write(result)
    fh:close()
end

-- `init_function` and `cleanup_function` pushed down to storages, but
-- `bench_prepare` called on the frontend
function bench.run(test_name, opts)
    -- allow to run under tarantool on 'space' configuration w/o test-run
    local conf_name = test_run and test_run:get_cfg('conf') or 'space'
    local conf_type = multirunner.get_conf(conf_name).type

    local iterations = opts.iterations[conf_type]
    assert(iterations ~= nil)

    -- checksum can be nil, 'not ok' will be reported
    local checksum = opts.checksums[conf_type]

    local result = multirunner.run_conf(conf_name, {
        test_run = test_run,
        init_function = opts.init_function,
        cleanup_function = opts.cleanup_function,
        workload = function(_, shard)
            return workload(shard, opts.bench_prepare, opts.bench_iter, {
                iterations = iterations,
                checksum = checksum,
                conf_type = conf_type,
            })
        end,
        servers = {'shard_tcp1', 'shard_tcp2', 'shard_tcp3', 'shard_tcp4'},
        use_tcp = true,
    })
    if result.ok then
        write_result(test_name, conf_name, result, not not test_run)
    end
end

-- helper for preparing benchmarking environment
function bench.bench_prepare_helper(testdata, shard)
    testdata.fill_test_data(shard or box.space)
    return test_utils.graphql_from_testdata(testdata, shard, {
        graphql_opts = {
            timeout_ms = graphql.TIMEOUT_INFINITY,
        }
    })
end

return bench
