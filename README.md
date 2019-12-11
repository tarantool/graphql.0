## Overview

[![Build Status][travis-ci-badge]][travis-ci-page]
[![Coverage Status][coveralls-badge]][coveralls-page]

[travis-ci-badge]: https://travis-ci.org/tarantool/graphql.svg?branch=master
[travis-ci-page]: https://travis-ci.org/tarantool/graphql
[coveralls-badge]: https://coveralls.io/repos/github/tarantool/graphql/badge.svg?branch=master
[coveralls-page]: https://coveralls.io/github/tarantool/graphql?branch=master

Set of adapters for GraphQL query language to the Tarantool data model. Based
on [graphql-lua](https://github.com/bjornbytes/graphql-lua).

[API documentation][apidoc].

[apidoc]: https://tarantool.github.io/graphql/

## Requirements

* For use:
  * tarantool,
  * lulpeg,
  * \>=tarantool/avro-schema-3.0.1,
  * \>=tarantool/shard-1.1-91-gfa88bf8 (but < 2.0) or
    \>=tarantool/shard-2.1-0-g0a7d98f (optional),
  * lrexlib-pcre2 or lrexlib-pcre (optional),
  * tarantool/http (optional, for GraphiQL).
* For test (additionally to 'for use'):
  * python 2.7,
  * virtualenv,
  * luacheck,
  * \>=tarantool/avro-schema-3.0.1,
  * \>=tarantool/shard-1.1-92-gec1a27e (but < 2.0) or
    \>=tarantool/shard-2.1-0-g0a7d98f,
  * lrexlib-pcre2 or lrexlib-pcre,
  * tarantool/http.
* For building apidoc (additionally to 'for use'):
  * ldoc.

## Usage

There are two ways to use the lib.

1) Use a default instance that generates a schema from local spaces (ones that
   have defined format):

```lua
local graphql = require('graphql')

local query = [[
    query user($user_id: String) {
        user_collection(user_id: $user_id) {
            user_id
            name
        }
    }
]]
local compiled_query = graphql.compile(query)

local variables = {user_id = 'user_id_1'}
local result = compiled_query:execute(variables)
```

2) Create an instance with a database schema in avro-schema format (see a
   [schema](test/testdata/nullable_1_1_conn_testdata.lua) for emails collection
   for example):

```lua
local graphql = require('graphql').new({
    schemas = schemas,
    collections = collections,
    accessor = accessor,
    service_fields = service_fields,
    indexes = indexes
})

local query = [[
    query user($user_id: String) {
        user_collection(user_id: $user_id) {
            user_id
            name
        }
    }
]]
local compiled_query = graphql:compile(query)

local variables = {user_id = 'user_id_1'}
local result = compiled_query:execute(variables)
```

## GraphiQL

```lua
local graphql = require('graphql').new({
    schemas = schemas,
    collections = collections,
    accessor = accessor,
    service_fields = service_fields,
    indexes = indexes
})

graphql:start_server()
-- now you can use GraphiQL interface at http://127.0.0.1:8080
graphql:stop_server()

-- as well you may do (with creating default instance underhood)
require('graphql').start_server()
```

## Sharding

The library can be configured to fetch tuples using tarantool/shard module:
`graphql.new(..., accessor = 'shard')`. The shard module should be configured
separately.

`require('graphql.storage').init()` should be called on each storage server to
use graphql with shard and BFS executor. Alternatively this executor can be
disabled with `graphql.new({..., use_bfs_executor = 'never'})` on a frontend
server.

The library has no built-in support of vshard module. Use `graphql.new(...,
accessor = 'shard', accessor_funcs = {<...>})` to adopt it if necessary.

## Mutations

### Mutations with space accessor

TBD: Describe which changes are transactional and which views are guaranteed to
be consistent.

### Mutations with shard accessor

Mutations are disabled in the resharding state of a shard cluster.

There are three types of modifications: insert, update and delete. Several
modifications are allowed in one GraphQL request, but they will be processed in
non-transactional way.

In the case of shard accessor the following constraints can guarantee that data
will be changed in atomic way or, in other words, in one shard request (but
foregoing and upcoming selects can see other data):

* One insert / update / delete argument over the entire GraphQL request.
* For update / delete: either the argument is for 1:1 connection or `limit: 1`
  is used for a collection (a upmost field) or 1:N connection (a nested
  field).
* No update of a first field of a **tuple** (shard key is calculated by it). It
  is the first field of upmost record in the schema for a collection in case
  when there are no service fields. If there are service fields, the first
  field of a tuple cannot be changed by a mutation GraphQL request.

Data can be changed between shard requests which are part of one GraphQL
request, so the result can observe inconsistent state. We'll don't show all
possible cases, but give an idea what going on in the following paragraph.

Filters are applied for an object(s) (several requests in case of filters by
connections, one request otherwise), then each object updated/deleted by its
primary key (one request per object), then all connected objects are resolved
in the same way.

### Insert

Example with an object passed from a variable:

```
mutation insert_user_and_order($user: user_collection_insert,
        $order: order_collection_insert) {
    user_collection(insert: $user) {
        user_id
        first_name
        last_name
    }
    order_collection(insert: $order) {
        order_id
        description
        in_stock
    }
}
```

Example with immediate argument for an object:

```
mutation insert_user_and_order {
    user_collection(insert: {
        user_id: "user_id_new_1"
        first_name: "Peter"
        last_name: "Petrov"
    }) {
        user_id
        first_name
        last_name
    }
    order_collection(insert: {
        order_id: "order_id_new_1"
        user_id: "user_id_new_1"
        description: "Peter's order"
        price: 0.0
        discount: 0.0
        # in_stock: true should be set as default value
    }) {
        order_id
        description
        in_stock
    }
}
```

Consider the following details:

* `${collection_name}_insert` is the name of the type whose value intended to
  pass to the `insert` argument. This type / argument requires a user to set
  all fields of an inserting object.
* Inserting cannot be used on connection fields, it is allowed only for
  top-level fields (named as well as collections).
* It is forbidden to use `insert` argument with any other argument.
* A mutation with an `insert` argument always return the object that was just
  inserted.
* Of course `insert` argument is forbidden in `query` requests.

### Update

Example with an update statement passed from a variable. Note that here we
update an object given by a connection (inside one of nested fields of a
request):

```
mutation update_user_and_order(
    $user_id: String
    $order_id: String
    $xuser: user_collection_update
    $xorder: order_collection_update
) {
    # update nested user
    order_collection(order_id: $order_id) {
        order_id
        description
        user_connection(update: $xuser) {
            user_id
            first_name
            last_name
        }
    }
    # update nested order (only the first, because of limit)
    user_collection(user_id: $user_id) {
        user_id
        first_name
        last_name
        order_connection(limit: 1, update: $xorder) {
            order_id
            description
            in_stock
        }
    }
}
```

Example with immediate argument for an update statement:

```
mutation update_user_and_order {
    user_collection(user_id: "user_id_1", update: {
        first_name: "Peter"
        last_name: "Petrov"
    }) {
        user_id
        first_name
        last_name
    }
    order_collection(order_id: "order_id_1", update: {
        description: "Peter's order"
        price: 0.0
        discount: 0.0
        in_stock: false
    }) {
        order_id
        description
        in_stock
    }
}
```

Consider the following details:

* `${collection_name}_update` is the name of the type whose value intended to
  pass to the `update` argument. This type / argument requires a user to set
  subset of fields of an updating object except primary key parts.
* A mutation with an `update` argument always return the updated object.
* The `update` argument is forbidden with `insert` or `delete` arguments.
* The `update` argument is forbidden in `query` requests.
* Objects are selected by filters first, then updated using a statement in the
  `update` argument, then connected objects are selected.
* The `limit` and `offset` arguments applied before update, so a user can use
  `limit: 1` to update only first match.
* Objects are traversed in pre-order depth-first way, object's fields are
  traversed in an order as they are written in a mutation request. So an
  `update` argument potentially changes those fields that are follows the
  updated object in this order.
* Filters by connected objects are performed before update. Resulting connected
  objects given after the update (it is matter when a field(s) of the parent
  objects by whose the connection is made is subject to change).

### Delete

Example:

```
mutation delete_user_and_order(
    $user_id: String,
    $order_id: String,
) {
    user_collection(user_id: $user_id, delete: true) {
        user_id
        first_name
        last_name
    }
    order_collection(order_id: $order_id, delete: true) {
        order_id
        description
        in_stock
    }
}
```

Consider the following details:

* There are no special type name for a `delete` argument, it is just Boolean.
* A mutation with a `delete: true` argument always return the deleted object.
* The `delete` argument is forbidden with `insert` or `update` arguments.
* The `delete` argument is forbidden in `query` requests.
* The same fields traversal order and 'select -> change -> select connected'
  order of operations for one field are applied likewise for the `update`
  argument.
* The `limit` argument can be used to define how many objects are subject to
  deletion and `offset` can help with adjusting start point of multi-object
  delete operation.

## Run tests

```
git clone https://github.com/tarantool/graphql.git
git submodule update --recursive --init
make test
```
To run specific test:
```
TEST_RUN_TESTS=common/mutation make test
```

## Hacking

Enable debug log:

```sh
export TARANTOOL_GRAPHQL_DEBUG=1
```

## Multi-head connections

A parent object is matching against a multi-head connection variants in the
order of the variants. The parent object should match with a determinant of
at least one variant except the following case. When source fields of all 
variants are null the multi-head connection obligated to give null object as 
the result. In this case the parent object is allowed to don’t match any variant. 
One can use this feature to avoid to set any specific determinant value when a 
multi-head connection is known to have no connected object.

## Notes on types

User should distinguish between Object and Map types. Both of them consists of
keys and values but there are some important differences.

While Object is a GraphQL built-in type, Map is a scalar-based type. In case of
Object-based type all key-value pairs are set during type definition and values
may have different types (as defined in the schema).

In contrast, set of valid Map keys is not defined in the schema, any key-value
pair is valid despite name of the key while value has schema-determined type
(which is the same among all values in the map).

Map-based types should be queried as a scalar type, not as an object type
(because map's keys are not part of the schema).

The following example works:

```
{
    …
    map_based_type
    …
}
```

The following example doesn't work:

```
{
    …
    map_based_type {
        key_1
    }
    …
}
```

## License

Consider LICENSE file for details. In brief:

* graphql/core: MIT (c) 2015 Bjorn Swenson
* graphql/server/graphiql: Facebook dev tools & examples license (allows use,
  copy and distribute) (c) 2015, Facebook, Inc (more: [1])
* all other content: BSD 2-clause (c) 2018 Tarantool AUTHORS

[1]: https://github.com/graphql/graphiql/issues/10
