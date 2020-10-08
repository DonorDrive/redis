component extends = "lib.util.tests.ContainerTestCase" {

	function afterTests() {
		variables.container.destroy();

		variables.connectionPool.close();
	}

	function beforeTests() {
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

		variables.container = new lib.redis.Cache(connectionPool = variables.connectionPool, name = "mxunit");
	}

	function test_serialize_deserialize() {
		local.value = ["foo", "", "bar"];
		variables.container.put("MxUnitTest_serialize_deserialize_array", local.value);
		local.result = variables.container.get("MxUnitTest_serialize_deserialize_array");
		// debug(local.result);
		assertEquals(local.value[1], local.result[1]);
		assertEquals(local.value[2], local.result[2]);
		assertEquals(local.value[3], local.result[3]);

		local.value = {foo = "bar", bing = "bong"};
		variables.container.put("MxUnitTest_serialize_deserialize_struct", local.value);
		local.result = variables.container.get("MxUnitTest_serialize_deserialize_struct");
		// debug(local.result);
		assertEquals(local.value.foo, local.result.foo);
		assertEquals(local.value.bing, local.result.bing);

		local.value = queryNew(
			"foo, bar",
			"integer, bit",
			[
				{ foo: 1, bar: true },
				{ foo: 2, bar: false },
				{ foo: 3, bar: true }
			]
		);
		variables.container.put("MxUnitTest_serialize_deserialize_query", local.value);
		local.result = variables.container.get("MxUnitTest_serialize_deserialize_query");
		// debug(local.result);
		for(local.i = 1; local.i <= 3; local.i++) {
			assertEquals(queryGetRow(local.value, local.i).foo, queryGetRow(local.result, local.i).foo);
			assertEquals(queryGetRow(local.value, local.i).bar, queryGetRow(local.result, local.i).bar);
		}

		local.value = createObject("java", "java.util.HashMap");
		try {
			variables.container.put("MxUnitTest_serialize_deserialize_object", local.value);
			fail("exception not thrown");
		} catch(lib.redis.InvalidDataTypeException e) {
			// this is the exception we want
		}
	}

}