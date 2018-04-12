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
  * >=tarantool/shard-1.1-91-gfa88bf8 (but < 2.0) (optional),
  * lrexlib-pcre2 or lrexlib-pcre (optional).
* For test (additionally to 'for use'):
  * python 2.7,
  * virtualenv,
  * luacheck,
  * >=tarantool/shard-1.1-92-gec1a27e (but < 2.0).
* For building apidoc (additionally to 'for use'):
  * ldoc.

## License

Consider LICENSE file for details. In brief:

* graphql/core: MIT (c) 2015 Bjorn Swenson
* all other content: BSD 2-clause (c) 2018 Tarantool AUTHORS
