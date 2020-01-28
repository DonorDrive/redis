component accessors = "true" {

	// github.com/xetorthio/jedis/wiki/Getting-started

	property name = "host" type = "string" setter = "false";
	property name = "master" type = "string";
	property name = "password" type = "string";
	property name = "poolConfiguration" type = "struct";
	property name = "port" type = "numeric";
	property name = "sentinels" type = "array";
	property name = "timeoutMillis" type = "numeric";

	ConnectionPool function init() {
		variables.port = 6379;
		variables.timeoutMillis = 1000;

		structEach(
			arguments,
			function(key, value) {
				invoke(this, "set#arguments.key#", [ arguments.value ]);
			}
		);

		return this;
	}

	void function close() {
		if(structKeyExists(variables, "pool")) {
			variables.pool.close();
		}
	}

	any function getConnection() {
		if(!structKeyExists(variables, "pool")) {
			throw(type = "lib.redis.UndefinedConnectionPoolException", message = "this ConectionPool must be open()'ed");
		}

		return variables.pool.getResource();
	}

	any function getJedisPool() {
		if(!structKeyExists(variables, "pool")) {
			throw(type = "lib.redis.UndefinedConnectionPoolException", message = "this ConectionPool must be open()'ed");
		}

		return variables.pool;
	}

	any function getJedisPoolConfig() {
		local.poolConfig = new java("redis.clients.jedis.JedisPoolConfig").init();

		if(structKeyExists(variables, "poolConfiguration")) {
			structEach(
				variables.poolConfiguration,
				function(key, value) {
					invoke(poolConfig, "set#arguments.key#", arguments.value);
				}
			);
		}

		return local.poolConfig;
	}

	ConnectionPool function open() {
		if(structKeyExists(variables, "pool")) {
			return variables.pool;
		}

		if(structKeyExists(variables, "sentinels")) {
			local.sentinels = new java("java.util.HashSet").init(variables.sentinels);

			if(structKeyExists(variables, "password")) {
				variables.pool = new java("redis.clients.jedis.JedisSentinelPool")
					.init(
						javaCast("string", variables.master),
						local.sentinels,
						getJedisPoolConfig(),
						javaCast("int", variables.timeoutMillis),
						javaCast("string", variables.password)
					);
			} else {
				variables.pool = new java("redis.clients.jedis.JedisSentinelPool")
					.init(
						javaCast("string", variables.master),
						local.sentinels,
						getJedisPoolConfig(),
						javaCast("int", variables.timeoutMillis)
					);
			}
		} else {
			if(structKeyExists(variables, "password")) {
				variables.pool = new java("redis.clients.jedis.JedisPool")
					.init(
						getJedisPoolConfig(),
						javaCast("string", variables.host),
						javaCast("int", variables.port),
						javaCast("int", variables.timeoutMillis),
						javaCast("string", variables.password)
					);
			} else {
				variables.pool = new java("redis.clients.jedis.JedisPool")
					.init(
						getJedisPoolConfig(),
						javaCast("string", variables.host),
						javaCast("int", variables.port),
						javaCast("int", variables.timeoutMillis)
					);
			}
		}

		return this;
	}

	ConnectionPool function setHost(required string host) {
		if(listLen(arguments.host, ":") == 2) {
			variables.port = listLast(arguments.host, ":");
		}

		variables.host = listFirst(arguments.host, ":");

		return this;
	}

}