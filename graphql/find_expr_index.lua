--[[

Temporary notes
---------------

Task: got result using index lookup(s) that are superset of the needed result.

- pattern matchers (match only what that is suitable for index lookup)
- support >, < and so on in find_index
- unit tests for find_index, try to make it reusable
- glue them all
- ----
- each node: suitable index parts: {index,field_no,iterator(=,!=,>,<,>=,<=),key}
- each node: suitable indexes: {index,iterator,key,full_match}
- each node: suitable index-sets: {{suitable indexes},full_match}
- construction:
  - index: // note: should be based on child index-sets, not indexes
    - and:
    - or:
  - index-set:
    - and:
    - or:
- full_match can be superseded with
  - for index: weight = covered child nodes / child nodes
  - for index-set: weight = index weights sum / child nodes
  - the above is wrong weights: it should not be about which part of an
    expr are covered, but about which fraction of the result we covered
- ----
- idea: doing all boolean arithmetic on parts, don't introduce
  index/index-set until end
- idea: we can construct DNF of parts (corr. to CNF of expr ops) during
  tree traversal by expanding and of ors / or of ands and passing negation
  down
- ----
- provide an iterator that merges several iterator results
- ----
- idea: construct DNF structure, where some conjuncts can be just expr, but
  some are saturated with index parts info
- it worth to support ranges: (x > V1 && x < V2) and so on; set index and stop
  key for it

]]--

--local expressions = require('graphql.expressions')

local find_expr_index = {}

--[[
local cmp_op_to_iterator_type = {
    ['=='] = 'EQ',
    ['!='] = 'NE', -- discarded after all NOT ops processing
    ['>']  = 'GT',
    ['>='] = 'GE',
    ['<']  = 'LT',
    ['<='] = 'LE',
}

-- 42 > x => x < 42
local reverse_iterator_type = {
    ['EQ'] = 'EQ',
    ['NE'] = 'NE',
    ['GT'] = 'LT',
    ['GE'] = 'LE',
    ['LT'] = 'GT',
    ['LE'] = 'GE',
}

-- !(x < 42) => x >= 42
local negate_iterator_type = {
    ['EQ'] = 'NE',
    ['NE'] = 'EQ',
    ['GT'] = 'LE',
    ['GE'] = 'LT',
    ['LT'] = 'GE',
    ['LE'] = 'GT',
}

local function is_cmp_op(op)
    return cmp_op_to_iterator_type[op] ~= nil
end
]]--

return find_expr_index
