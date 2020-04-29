# redis
A ColdFusion wrapper for Jedis and JRediSearch

## Motivation
DonorDrive leverages distributed caching extensively throughout our platform. Redis is a very popular engine for caching, and the RediSearch module makes that cache searchable through a verbose query syntax.

## Getting Started
The `redis` package assumes that it will reside in a `lib` directory under the web root, or mapped in the consuming application. You *must* also use our `lib.util` package for this to work (https://github.com/DonorDrive/util). In order to leverage the `IQueryable` interface for cache searching, you must also grab our `sql` project (https://github.com/DonorDrive/sql).

The `jedis` jar that ships with ACF must be updated to version 3.1.0 (note: we don't leverage any of the native CF-Redis functionality, so backward compatibility has not been tested). To leverage RediSearch via the `QueryableCache` component, you must also have JRediSearch 1.3.0 or higher installed.

### How do I use this?
Redis runs as a separate service for CF to connect to. This is controlled via the ConnectionPool component:

```
connectionPool = new lib.redis.ConnectionPool()
	.setHost("127.0.0.1:6379")
	.open();
```

For Redis Sentinel, you may construct your ConnectionPool as such:

```
connectionPool = new lib.redis.ConnectionPool()
	.setMaster("redis-cluster")
	.setSentinels([
		"127.0.0.1:6379",
		...
	])
	.open();
```

Once created, the ConnectionPool may be leveraged to instantiate `Cache` or `QueryableCache`:

```
cache = new lib.redis.Cache(connectionPool, "mycache");

cache.put("foo", "bar");
```

The preceeding would result in a key of `mycache:foo` with a value of `bar` being inserted into the connected Redis instance. The `Cache` client currently supports native CF types (serializing arrays and structs as JSON on `put()`).

For a more in-depth examples, please refer to the unit tests.