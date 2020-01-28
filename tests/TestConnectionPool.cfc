component extends = "mxunit.framework.TestCase" {

	function afterTests() {
		variables.connectionPool.close();
	}

	function beforeTests() {
		try {
			variables.connectionPool = new lib.redis.ConnectionPool()
				.setHost("127.0.0.1:6379")
/*
				.setMaster("MASTER-NAME")
				.setPassword("PASSWORD")
				.setSentinels([
					"SENTINEL-IP-1",
					"SENTINEL-IP-2",
					...
					"SENTINEL-IP-N"
				])
 */
				.open();
		} catch(Any e) {
			variables.exception = e;
		}
	}

	function test_getConnection() {
//		debug(variables.exception); return;
		local.redis = variables.connectionPool.getConnection();

//		debug(local.redis);
		assertEquals("redis.clients.jedis.Jedis", getMetadata(local.redis).getCanonicalName());

		local.redis.close();
	}

	function test_getJedisPool() {
		local.jedisPool = variables.connectionPool.getJedisPool();

//		debug(local.jedisPool);
		assertEquals("redis.clients.jedis.JedisPool", getMetadata(local.jedisPool).getCanonicalName());
	}

}