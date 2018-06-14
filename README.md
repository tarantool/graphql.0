## Overview

[![Build Status](https://travis-ci.org/tarantool/graphql.svg?branch=master)](https://travis-ci.org/tarantool/graphql)

Set of adapters for GraphQL query language to the Tarantool data model. Based
on [graphql-lua](https://github.com/bjornbytes/graphql-lua).

The library split to the two parts: GraphQL parser/executor and data accessors.
The GraphQL part defines possible shapes of queries and implements abstract
query executor, but data accessors implement fetching of objects.

### GraphQL part

The GraphQL part operates on *collections* of *objects*. Each collection stored
set of object of specific type. Collections are linked between using
*connections* in order to implement JOINs (looks like nesting one object to
another in a GraphQL query result).

Object types abstracted away from how it stored in database and described using
avro-schema format. Each collection is described using avro schema name and set
of connections, which holds names of source and destination fields must be
equal in linked objects.

A shape of a query is following. Top-level fields are named as collections.
Nested fields named by a connection name.

GraphQL provides facility to filter objects using arguments. Top-level objects
have set of arguments that match fields set (except nested ones derived from
connections). Nested fields (which derived from connections) have arguments set
defined by a data accessor, typically they are support filtering and
pagination.

Nested fields have object type or list of objects type depending of
corresponding connection type: 1:1 or 1:N.

### Data accessor part

Data accessor defines how objects are stored and how connections are
implemented, also it defines set of arguments for connections. So, data
accessors operates on avro schemas, collections and service fields to fetch
objects and connections and indexes to implement JOINing (nesting).

Note: service fields is metadata of an object that is stored, but is not part
of the object.

Currently only *space* data accessor is implemented. It allows to execute
GraphQL queries on data from the local Tarantool's storage called spaces.

It is planned to implement another data accessor that allows to fetch objects
sharded using tarantool/shard module.

### Notes on types

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

## Usage

There are two ways to use the lib.
1) Create an instance and use it (detailed examples may be found in /test):
```
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
2) Use the lib itself (it will create a default instance underhood. As no
avro-schema is given, GraphQL schemas will be generated from results
box.space.some_space:format()):
```
local graphql_lib = require('graphql')
-- considering the same query and variables

local compiled_query = graphql_lib.compile(query)
local result = compiled_query:execute(variables)
```

### Mutations

Mutations are disabled for avro-schema-2\*, because it can work incorrectly for
schemas with nullable types (ones whose marked with asterisk). Mutations still
can be enabled with the `enable_mutations = true` option, but use it with
caution. Don't enable this option with schemas involve nullable types.

#### Mutations with space accessor

TBD: Describe which changes are transactional and which views are guaranteed to
be consistent.

#### Mutations with shard accessor

Mutations are disabled in the resharding state of a shard cluster.

There are three types of modifications: insert, update and delete. Several
modifications are allowed in an one GraphQL request, but they will be processed
in non-transactional way.

In the case of shard accessor the following constraints can guarantee that data
will be changed in atomic way or, in other words, in an one shard request (but
foregoing and upcoming selects can see other data):

* One insert / update / delete argument over the entire GraphQL request.
* For update / delete: either the argument is for 1:1 connection or `limit: 1`
  is used for a collection (a topmost field) or 1:N connection (a nested
  field).
* No update of a first field of a **tuple** (shard key is calculated by it). It
  is the first field of upmost record in the schema for a collection in case
  when there are no service fields. If there are service fields, the first
  field of a tuple cannot be changed by a mutation GraphQL request.

Data can be changed between shard requests which are part of the one GraphQL
request, so the result can observe inconsistent state. We'll don't show all
possible cases, but give an idea what going on in the following paragraph.

Filters are applied for an object(s) (several requests in case of filters by
connections, one request otherwise), then each object updated/deleted by its
primary key (one request per object), then all connected objects are resolved
in the same way.

#### Insert

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

#### Update

Example with an update statement passed from a variable. Note that here we
update an object given by a connection (inside an one of nested fields of a
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
* Objects traversed in deep-first up-first order as it written in a mutation
  request. So an `update` argument potentially changes those fields that are
  follows the updated object in this order.
* Filters by connected objects are performed before update. Resulting connected
  objects given after the update (it is matter when a field(s) of the parent
  objects by whose the connection is made is subject to change).

#### Delete

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
  order of operations for an one field are applied likewise for the `update`
  argument.
* The `limit` argument can be used to define how many objects are subject to
  deletion and `offset` can help with adjusting start point of multi-object
  delete operation.

## GraphiQL
```
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

## Run tests

```
git clone https://github.com/tarantool/graphql.git
git submodule update --recursive --init
make test
```

## Requirements

* For use:
  * tarantool,
  * lulpeg,
  * >=tarantool/avro-schema-2.0-71-gfea0ead,
  * >=tarantool/shard-1.1-91-gfa88bf8 (but < 2.0) or
    >=tarantool/shard-2.1-0-g0a7d98f (optional),
  * lrexlib-pcre2 or lrexlib-pcre (optional),
  * tarantool/http (optional, for GraphiQL).
* For test (additionally to 'for use'):
  * python 2.7,
  * virtualenv,
  * luacheck,
  * >=tarantool/avro-schema-2.2.2-4-g1145e3e,
  * >=tarantool/shard-1.1-92-gec1a27e (but < 2.0) or
    >=tarantool/shard-2.1-0-g0a7d98f,
  * tarantool/http.
* For building apidoc (additionally to 'for use'):
  * ldoc.

## License

Consider LICENSE file for details. In brief:

* graphql/core: MIT (c) 2015 Bjorn Swenson
* graphql/server/graphiql: Facebook dev tools & examples license (allows use,
  copy and distribute) (c) 2015, Facebook, Inc (more: [1])
* all other content: BSD 2-clause (c) 2018 Tarantool AUTHORS

[1]: https://github.com/graphql/graphiql/issues/10
