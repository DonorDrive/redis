component accessors = "true" {

	// github.com/xetorthio/jedis/wiki/Getting-started

	property name = "clientName" type = "string";
	property name = "host" type = "string" setter = "false";
	property name = "master" type = "string";
	property name = "password" type = "string";
	property name = "pool" type = "struct";
	property name = "port" type = "numeric";
	property name = "sentinelPassword" type = "string";
	property name = "sentinels" type = "array";
	property name = "socketTimeoutMillis" type = "numeric";
	property name = "timeoutMillis" type = "numeric";

	ConnectionPool function init() {
		variables.protocol = createObject("java", "redis.clients.jedis.Protocol");

		// set some defaults
		variables.clientName = createObject("java", "java.net.InetAddress").getLocalHost().getHostName();
		variables.host = variables.protocol.DEFAULT_HOST;
		variables.port = variables.protocol.DEFAULT_PORT;
		variables.socketTimeoutMillis = variables.protocol.DEFAULT_TIMEOUT;
		variables.timeoutMillis = variables.protocol.DEFAULT_TIMEOUT;

		structEach(
			arguments,
			function(key, value) {
				invoke(this, "set#arguments.key#", [ arguments.value ]);
			}
		);

		return this;
	}

	void function close() {
		if(structKeyExists(variables, "jedisPool")) {
			variables.jedisPool.close();
			structDelete(variables, "jedisPool");
		}
	}

	any function getConnection() {
		if(!structKeyExists(variables, "jedisPool")) {
			throw(type = "lib.redis.UndefinedConnectionPoolException", message = "this ConectionPool must be open()'ed");
		}

		return variables.jedisPool.getResource();
	}

	any function getJedisPool() {
		if(!structKeyExists(variables, "jedisPool")) {
			throw(type = "lib.redis.UndefinedConnectionPoolException", message = "this ConectionPool must be open()'ed");
		}

		return variables.jedisPool;
	}

	any function getJedisPoolConfig() {
		local.jedisPoolConfig = createObject("java", "redis.clients.jedis.JedisPoolConfig").init();

		if(structKeyExists(variables, "pool")) {
			structEach(
				variables.pool,
				function(key, value) {
					invoke(jedisPoolConfig, "set#arguments.key#", arguments.value);
				}
			);
		}

		return local.jedisPoolConfig;
	}

	boolean function isClustered() {
		return structKeyExists(variables, "sentinels");
	}

	ConnectionPool function open() {
		if(structKeyExists(variables, "jedisPool")) {
			return this;
		}

		if(structKeyExists(variables, "sentinels")) {
			variables.jedisPool = createObject("java", "redis.clients.jedis.JedisSentinelPool")
				.init(
					javaCast("string", variables.master),
					createObject("java", "java.util.HashSet").init(variables.sentinels),
					getJedisPoolConfig(),
					javaCast("int", variables.timeoutMillis),
					javaCast("int", variables.socketTimeoutMillis),
					structKeyExists(variables, "password") ? javaCast("string", variables.password) : javaCast("null", ""),
					variables.protocol.DEFAULT_DATABASE,
					javaCast("string", variables.clientName),
					javaCast("int", variables.timeoutMillis), // sentinelConnectionTimeout
					javaCast("int", variables.socketTimeoutMillis), // sentinelSoTimeout
					structKeyExists(variables, "sentinelPassword") ? javaCast("string", variables.sentinelPassword) : javaCast("null", ""),
					javaCast("string", "sentinel:" & variables.clientName)
				);
		} else {
			variables.jedisPool = createObject("java", "redis.clients.jedis.JedisPool")
				.init(
					getJedisPoolConfig(),
					javaCast("string", variables.host),
					javaCast("int", variables.port),
					javaCast("int", variables.timeoutMillis),
					javaCast("int", variables.socketTimeoutMillis),
					structKeyExists(variables, "password") ? javaCast("string", variables.password) : javaCast("null", ""),
					variables.protocol.DEFAULT_DATABASE,
					javaCast("string", variables.clientName)
				);
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