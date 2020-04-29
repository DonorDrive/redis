component accessors = "true" implements = "lib.util.IContainer" {

	property name = "timeToLiveSeconds" type = "numeric";

	lib.redis.Cache function init(required lib.redis.ConnectionPool connectionPool, required string name) {
		variables.connectionPool = arguments.connectionPool;
		variables.name = lCase(arguments.name);

		return this;
	}

	void function clear() {
		try {
			local.connection = variables.connectionPool.getConnection();

			local.keys = local.connection.keys(getName() & ":*").toArray(javaCast("String[]", []));

			local.connection.del(local.keys);
		} catch(redis.clients.jedis.exceptions.JedisExhaustedPoolException e) {
			// nothing rn
		} finally {
			if(structKeyExists(local, "connection")) {
				local.connection.close();
			}
		}
	}

	boolean function containsKey(required string key) {
		try {
			local.connection = variables.connectionPool.getConnection();

			return local.connection.exists(getKey(arguments.key));
		} catch(redis.clients.jedis.exceptions.JedisExhaustedPoolException e) {
			// nothing rn
		} finally {
			if(structKeyExists(local, "connection")) {
				local.connection.close();
			}
		}

		return false;
	}

	private any function deserialize(required string value) {
		if(find("|", arguments.value) > 0) {
			switch(left(arguments.value, find("|", arguments.value))) {
				case "~*array|":
					arguments.value = listToArray(mid(arguments.value, find("|", arguments.value) + 1, len(arguments.value) - find("|", arguments.value)), chr(31), true);
					break;
				case "~*struct|":
				case "~*query|":
					arguments.value = deserializeJSON(mid(arguments.value, find("|", arguments.value) + 1, len(arguments.value) - find("|", arguments.value)), false);
					break;
				default:
					break;
			}
		}

		return arguments.value;
	}

	void function destroy() {
		return clear();
	}

	any function get(required string key) {
		try {
			local.connection = variables.connectionPool.getConnection();

			local.item = local.connection.get(getKey(arguments.key));

			if(structKeyExists(local, "item")) {
				return variables.deserialize(local.item);
			}
		} catch(redis.clients.jedis.exceptions.JedisExhaustedPoolException e) {
			// nothing rn
		} finally {
			if(structKeyExists(local, "connection")) {
				local.connection.close();
			}
		}
	}

	private string function getKey(required string key) {
		arguments.key = lCase(arguments.key);

		if(left(arguments.key, len( getName() & ":" )) != ( getName() & ":" )) {
			arguments.key = getName() & ":" & arguments.key;
		}

		return arguments.key;
	}

	string function getName() {
		return variables.name;
	}

	boolean function isEmpty() {
		try {
			local.connection = variables.connectionPool.getConnection();

			local.scanParams = createObject("java", "redis.clients.jedis.ScanParams")
				// setting count > 1 forces scan to do more scanning
				.count(100)
				.match(getName() & "\:*");

			local.scanResult = local.connection.scan(
				local.scanParams.SCAN_POINTER_START,
				local.scanParams
			);

			return (local.scanResult.getResult().size() == 0);
		} catch(redis.clients.jedis.exceptions.JedisExhaustedPoolException e) {
			rethrow;
			// nothing rn
		} finally {
			if(structKeyExists(local, "connection")) {
				local.connection.close();
			}
		}

		return false;
	}

	string function keyList() {
		try {
			local.connection = variables.connectionPool.getConnection();

			local.keys = local.connection.keys(getName() & ":*");
			local.ret = [];
			local.iterator = local.keys.iterator();

			while(local.iterator.hasNext()) {
				arrayAppend(
					local.ret,
					listRest(local.iterator.next(), ":")
				);
			}

			arraySort(local.ret, "textnocase");

			return arrayToList(local.ret);
		} catch(redis.clients.jedis.exceptions.JedisExhaustedPoolException e) {
			// nothing rn
		} finally {
			if(structKeyExists(local, "connection")) {
				local.connection.close();
			}
		}

		return "";
	}

	void function put(required string key, required any value) {
		arguments.value = variables.serialize(arguments.value);

		try {
			local.connection = variables.connectionPool.getConnection();

			if(structKeyExists(arguments, "timeToLiveSeconds")) {
				local.connection.setex(getKey(arguments.key), javaCast("int", arguments.timeToLiveSeconds), arguments.value);
			} else if(structKeyExists(variables, "timeToLiveSeconds")) {
				local.connection.setex(getKey(arguments.key), javaCast("int", variables.timeToLiveSeconds), arguments.value);
			} else {
				local.connection.set(getKey(arguments.key), arguments.value);
			}
		} catch(redis.clients.jedis.exceptions.JedisExhaustedPoolException e) {
			// nothing rn
		} finally {
			if(structKeyExists(local, "connection")) {
				local.connection.close();
			}
		}
	}

	void function putAll(required struct values, boolean clear = false, boolean overwrite = false) {
		try {
			local.connection = variables.connectionPool.getConnection();

			if(arguments.clear) {
				local.keys = local.connection.keys(getName() & ":*").toArray(javaCast("String[]", []));

				local.connection.del(local.keys);
			} else if(arguments.overwrite) {
				local.connection.del(javaCast("String[]", structKeyArray(arguments.values)));
			}

			for(local.key in arguments.values) {
				if(!arguments.overwrite && local.connection.exists(getKey(local.key))) {
					continue;
				}

				if(structKeyExists(variables, "timeToLiveSeconds")) {
					local.connection.setex(getKey(local.key), javaCast("int", variables.timeToLiveSeconds), variables.serialize(arguments.values[local.key]));
				} else {
					local.connection.set(getKey(local.key), variables.serialize(arguments.values[local.key]));
				}
			}
		} catch(redis.clients.jedis.exceptions.JedisExhaustedPoolException e) {
			// nothing rn
		} finally {
			if(structKeyExists(local, "connection")) {
				local.connection.close();
			}
		}
	}

	void function remove(required string key) {
		try {
			local.connection = variables.connectionPool.getConnection();

			local.connection.del(getKey(arguments.key));
		} catch(redis.clients.jedis.exceptions.JedisExhaustedPoolException e) {
			// nothing rn
		} finally {
			if(structKeyExists(local, "connection")) {
				local.connection.close();
			}
		}
	}

	private string function serialize(required any value) {
		if(!structKeyExists(arguments, "value")) {
			return "";
		}

		if(isArray(arguments.value)) {
			return "~*array|" & arrayToList(arguments.value, chr(31));
		} else if(isStruct(arguments.value) && !isObject(arguments.value)) {
			return "~*struct|" & serializeJSON(arguments.value);
		} else if(isQuery(arguments.value)) {
			return "~*query|" & serializeJSON(arguments.value);
		} else if(isSimpleValue(arguments.value)) {
			return arguments.value;
		}

		throw(type = "lib.redis.InvalidDataTypeException", message = "The value is not a supported data type.");
	}

	struct function values() {
		local.return = {};

		try {
			local.connection = variables.connectionPool.getConnection();

			local.keys = local.connection.keys(getName() & ":*").toArray(javaCast("String[]", []));

			for(local.key in local.keys) {
				local.return[listRest(local.key, ":")] = variables.deserialize(local.connection.get(local.key));
			}
		} catch(redis.clients.jedis.exceptions.JedisExhaustedPoolException e) {
			// nothing rn
		} finally {
			if(structKeyExists(local, "connection")) {
				local.connection.close();
			}
		}

		return local.return;
	}

}