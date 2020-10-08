component extends = "mxunit.framework.TestCase" {

	function afterTests() {
		variables.cache.dropIndex();
	}

	function beforeTests() {
		try {
			variables.connectionPool = new lib.redis.ConnectionPool()
				.setHost("127.0.0.1:6379")
				.open();

			variables.cache = new lib.redis.QueryableCache(connectionPool = variables.connectionPool, name = "mxunit:indexed");

			variables.query = queryNew(
				"id, createdTimestamp, createdDate, createdTime, foo, bar, letter, floatie",
				"integer, timestamp, date, time, varchar, bit, varchar, double"
			);

			queryAddRow(
				variables.query,
				[
					{
						"id": 5001,
						"bar": true,
						"foo": "9E835384-07E9-724C-1911F370FCFA67A6",
						"createdTimestamp": parseDateTime("2019-12-12T00:00:00.123Z"),
						"createdDate": createDate(2019, 12, 12),
						"createdTime": createTime(0, 0, 0),
						"letter": "A",
						"floatie": 10000000.00
					},
					{
						"id": 5002,
						"bar": true,
						"foo": "ECDC79F4-A719-D461-81BEC1A394697213",
						"createdTimestamp": parseDateTime("2019-12-25T00:00:00.001Z"),
						"createdDate": createDate(2019, 12, 25),
						"createdTime": createTime(0, 0, 0),
						"letter": "B",
						"floatie": 1.00
					},
					{
						"id": 5003,
						"bar": true,
						"foo": "EFA8C662-B0A9-FD1D-99FB38E815EDA2C5",
						"createdTimestamp": parseDateTime("2019-12-31T00:00:00.000Z"),
						"createdDate": createDate(2019, 12, 31),
						"createdTime": createTime(0, 0, 0),
						"letter": "C",
						"floatie": -1.300001
					}
				]
			);

			variables.now = now();
			for(local.i = 4; local.i <= 1000; local.i++) {
				queryAddRow(
					variables.query,
					{
						"id": 5000 + local.i,
						"bar": (!randRange(1, 3) % 2 ? local.i % 2 : javaCast("null", "")),
						"foo": (local.i == 500 ? "Šťŕĭńġ" : local.i == 501 ? "the rain in spain" : local.i == 502 ? "6k" : createUUID()),
						"createdTimestamp": (!randRange(1, 3) % 2 ? dateAdd("s", -(local.i), variables.now) : javaCast("null", "")),
						"createdDate": (!randRange(1, 3) % 2 ? variables.now : javaCast("null", "")),
						"createdTime": (!randRange(1, 3) % 2 ? variables.now : javaCast("null", "")),
						"letter": chr(64 + randRange(1, 26) + (local.i % 2 ? 32 : 0)),
						"floatie": 0.01
					}
				);
			}

			variables.queryable = new lib.sql.QueryOfQueries(variables.query).setIdentifierField("id");

			variables.cache.setQueryable(variables.queryable);
		} catch(Any e) {
			variables.exception = e;
		}
	}

	function test_0_createIndex() {
//		debug(variables.exception); return;

		try{
			// create the index
			variables.cache.createIndex();
		} catch(redis.clients.jedis.exceptions.JedisDataException e) {
			// index exists already, do nothing
		}

		local.info = variables.cache.getInfo();
//		debug(local.info);
		assertEquals("mxunit:indexed", local.info["index_name"]);
	}

	function test_0_seedFromQueryable() {
		variables.cache.seedFromQueryable();
	}

	function test_containsRow() {
		assertTrue(variables.cache.containsRow(id = variables.query.id[1]));
		assertFalse(variables.cache.containsRow(id = "boo-boo-butt"));
	}

	function test_fieldExists() {
		assertTrue(variables.cache.fieldExists("id"));
		assertFalse(variables.cache.fieldExists("asdfasfasdf"));
	}

	function test_fieldIsFilterable() {
		assertTrue(variables.cache.fieldIsFilterable("id"));
		assertFalse(variables.cache.fieldIsFilterable("asdfasfasdf"));
	}

	function test_getFieldList() {
		assertEquals(variables.queryable.getFieldList(), variables.cache.getFieldList());
	}

	function test_getFieldSQL() {
		assertEquals("", variables.cache.getFieldSQL("id"));
	}

	function test_getFieldSQLType() {
		assertEquals("integer", variables.cache.getFieldSQLType("id"));
		assertEquals("timestamp", variables.cache.getFieldSQLType("createdTimestamp"));
		assertEquals("varchar", variables.cache.getFieldSQLType("foo"));
		assertEquals("bit", variables.cache.getFieldSQLType("bar"));
	}

	function test_getRowKey() {
		assertEquals("mxunit:indexed:id:5001", variables.cache.getRowKey(id = 5001));
	}

	function test_putRow_getRow() {
		local.compareDate = createDateTime(1948, 12, 10, 10, 0, 0);
		local.queryRow = queryGetRow(variables.query, 1);
		local.queryRow.createdTimestamp = local.compareDate;
		variables.cache.putRow(local.queryRow);
		local.compare = variables.cache.getRow(argumentCollection = local.queryRow);

		assertEquals(0, dateCompare(local.compareDate, local.compare.createdTimestamp));
	}

	function test_putRow_getRow_DDMAINT_26680() {
		local.queryRow = queryGetRow(variables.query, 1);
		local.queryRow.foo = "";
		variables.cache.putRow(local.queryRow);
		local.compare = variables.cache.getRow(argumentCollection = local.queryRow);

		assertEquals("", local.compare.foo);
	}

	function test_putRow_getRow_DDMAINT_27043() {
		local.queryRow = queryGetRow(variables.query, 1);
		local.queryRow.createdTimestamp = "";
		variables.cache.putRow(local.queryRow);
		local.compare = variables.cache.getRow(argumentCollection = local.queryRow);

		assertEquals("", local.compare.createdTimestamp);
	}

	function test_removeRow() {
		local.queryRow = queryGetRow(variables.query, 1);
		local.queryRow.foo = createUUID();
		variables.cache.putRow(local.queryRow);

		assertTrue(variables.cache.containsRow(id = local.queryRow.id));
		variables.cache.removeRow(id = local.queryRow.id);
		assertFalse(variables.cache.containsRow(id = local.queryRow.id));
	}

	function test_seedFromQueryable_overwrite_where() {
		variables.cache.seedFromQueryable();

		local.row = queryGetRow(variables.query, 2);
		local.row.foo = createUUID();

		variables.cache.putRow(local.row);

//		debug(local.row);

		variables.cache.seedFromQueryable(overwrite = true, where = "id = #local.row.id#");

		local.overwriteElement = variables.cache.getRow(id = local.row.id);

//		debug(local.overwriteElement);

		assertNotEquals(local.row.foo, local.overwriteElement.foo);
	}

	function test_select() {
		local.select = variables.cache.select();
//		debug(local.select);

		local.result = local.select.execute();
//		debug(local.result);

		assertEquals(1000, local.result.recordCount);
	}

	function test_select_aggregate() {
		try {
			variables.cache.select("SUM(id)").execute();
			fail("should not be here");
		} catch(Any e) {
			assertEquals("lib.redis.UnsupportedOperationException", e.type);
		}
	}

	function test_select_orderBy_DD_13345() {
		local.result = variables.cache.select().orderBy("bar DESC, id ASC").execute(limit = 10);

		debug(local.result);
		assertEquals(10, local.result.recordCount);
	}

	function test_select_orderBy_DD_13346() {
		local.result = variables.cache.select("letter, id").orderBy("letter ASC, id DESC").execute();

		debug(local.result);
	}

	function test_select_orderBy_limit() {
		local.result = variables.cache.select().orderBy("id DESC").execute(limit = 10);

		debug(local.result);
		assertEquals(10, local.result.recordCount);
		assertEquals("6000,5999,5998,5997,5996,5995,5994,5993,5992,5991", valueList(local.result.id));
	}

	function test_select_orderBy_limit_offset() {
		local.result = variables.cache.select().orderBy("id ASC").execute(limit = 10, offset = 10);

		debug(local.result);
		assertEquals(10, local.result.recordCount);
		assertEquals(1000, local.result.getMetadata().getExtendedMetadata().totalRecordCount);
		assertEquals("5011,5012,5013,5014,5015,5016,5017,5018,5019,5020", valueList(local.result.id));
	}

	function test_select_where() {
		local.compare = queryGetRow(variables.query, variables.query.recordCount);
		local.result = variables.cache.select().where("id = #local.compare.id#").execute();

		debug(local.result);
		assertEquals(local.compare.id, local.result.id);
		assertEquals(local.compare.createdTimestamp, local.result.createdTimestamp);
		assertEquals(local.compare.createdDate, local.result.createdDate);
		assertEquals(local.compare.createdTime, local.result.createdTime);
		assertEquals(1, local.result.recordCount);
	}

	function test_select_where_compound_limit() {
		local.result = variables.cache.select().where("letter IN (A,B,C) AND createdTimestamp >= '#dateTimeFormat(dateAdd("d", -1, variables.now), "yyyy-mm-dd HH:nn:ss.l")#'").execute(limit = 10);

//		debug(local.result);
		assertEquals(10, local.result.recordCount);
	}

	function test_select_where_DD_12824() {
		local.result = variables.cache.select().where("createdTimestamp < '#dateTimeFormat(now(), 'yyyy-mm-dd HH:nn:ss.l')#' AND bar = 1").execute();

		debug(local.result);
	}

	function test_select_where_DD_13763() {
		local.where = "foo LIKE '%#listFirst(lCase(variables.query.foo[1]), '-')#%'";
		local.result = variables.cache.select().where(local.where).orderBy("id ASC").execute();

		assertTrue(local.result.recordCount >= 1);

		debug(local.where);
		debug(local.result);
	}

	function test_select_where_DD_16368() {
		local.where = "foo LIKE '%rain in spain%'";
		local.result = variables.cache.select().where(local.where).execute();

		assertEquals(5501, local.result.id);

		local.where = "foo LIKE '%the rain in spain%'";
		local.result = variables.cache.select().where(local.where).execute();

		assertEquals(5501, local.result.id);

		local.where = "foo LIKE '%a in the%'";
		local.result = variables.cache.select().where(local.where).execute();

		assertEquals(0, local.result.recordCount);
	}

	function test_select_where_DDMAINT_20768() {
		local.where = "createdTimestamp >= '#dateTimeFormat(dateAdd("s", -10, variables.now), "yyyy-mm-dd HH:nn:ss")#'";

		local.assert = variables.queryable.select().where(local.where).orderBy("id ASC").execute();
		local.result = variables.cache.select().where(local.where).orderBy("id ASC").execute();

		debug(local.assert);
		debug(local.result);

		assertEquals(local.assert, local.result);
	}

	function test_select_where_DDMAINT_21917() {
		local.where = "foo = 'Šťŕĭńġ'";
		local.result = variables.cache.select().where(local.where).execute();

		assertEquals(1, local.result.recordCount);
		assertEquals(5500, local.result.id);

		debug(local.where);
		debug(local.result);
	}


	function test_select_where_DDMAINT_27088_KEEP_FIELD_FLAGS() {
		local.where = "letter = 'Šťŕĭńġ'";
		local.result = variables.cache.select().where(local.where).execute();

		assertEquals(0, local.result.recordCount);

		debug(local.where);
		debug(local.result);
	}

	function test_select_where_DDMAINT_27088_KEEP_FIELD_FLAGS_2() {
		local.where = "foo IN (6k)";
		local.result = variables.cache.select().where(local.where).execute();

		assertEquals(1, local.result.recordCount);

		debug(local.where);
		debug(local.result);
	}

	function test_select_where_DDMAINT_27088_USE_TERM_OFFSETS() {
		local.where = "foo = 'the rain in spain'";
		local.result = variables.cache.select().where(local.where).execute();

		assertEquals(1, local.result.recordCount);

		debug(local.where);
		debug(local.result);

		local.where = "foo = 'the spain in rain'";
		local.result = variables.cache.select().where(local.where).execute();

		assertEquals(0, local.result.recordCount);
	}

	function test_select_where_in() {
		local.result = variables.cache.select("id, foo").where("foo IN ('#variables.query.foo[1]#', '#variables.query.foo[2]#', '#variables.query.foo[3]#') OR id IN (5005, 5010, 5015)").execute();

//		debug(local.result);
		assertEquals("foo,id", listSort(local.result.columnList, "textnocase"));
		assertEquals("5001,5002,5003,5005,5010,5015", listSort(valueList(local.result.id), "numeric"));
		assertEquals(6, local.result.recordCount);

		local.result = variables.cache.select("id, foo").where("foo IN ('#variables.query.foo[1]#','#variables.query.foo[2]#')").execute();

//		debug(local.result);
		assertEquals("foo,id", listSort(local.result.columnList, "textnocase"));
		assertEquals("5001,5002", listSort(valueList(local.result.id), "numeric"));

		// test a single record
		local.result = variables.cache.select("id, foo").where("foo IN ('#variables.query.foo[1]#'").execute();

//		debug(local.result);
		assertEquals(1, local.result.recordCount);
	}

	function test_select_where_not_in() {
		// numeric filtering
		local.result = variables.cache.select("id, foo").where("id NOT IN (5005, 5010, 5012)").orderBy("id ASC").execute(limit = 10);

//		debug(local.result);
		assertEquals("foo,id", listSort(local.result.columnList, "textnocase"));
		assertEquals("5001,5002,5003,5004,5006,5007,5008,5009,5011,5013", listSort(valueList(local.result.id), "numeric"));

		// string filtering
		local.result = variables.cache.select("id, foo").where("foo NOT IN ('#variables.query.foo[1]#', '#variables.query.foo[2]#', '#variables.query.foo[3]#')").orderBy("id ASC").execute(limit = 10);

		debug(local.result);
		assertEquals("foo,id", listSort(local.result.columnList, "textnocase"));
		assertEquals("5004,5005,5006,5007,5008,5009,5010,5011,5012,5013", listSort(valueList(local.result.id), "numeric"));

		// test negation of a single record
		local.result = variables.cache.select("id, foo").where("foo NOT IN ('#variables.query.foo[1]#'").orderBy("id ASC").execute(limit = 10);

//		debug(local.result);
		assertEquals("foo,id", listSort(local.result.columnList, "textnocase"));
		assertEquals("5002,5003,5004,5005,5006,5007,5008,5009,5010,5011", listSort(valueList(local.result.id), "numeric"));
		assertEquals(10, local.result.recordCount);
	}

	function test_select_where_orderBy_limit() {
		local.result = variables.cache.select().where("id <= 5500").orderBy("id DESC").execute(limit = 10);

//		debug(local.result);
		assertEquals("5500,5499,5498,5497,5496,5495,5494,5493,5492,5491", valueList(local.result.id));
		assertEquals(10, local.result.recordCount);
	}

	function test_toRediSearchDocument_fromRediSearchDocument() {
		local.row = queryGetRow(variables.query, 1);

		local.document = variables.cache.toRediSearchDocument(local.row);

		assertEquals(local.row.letter, local.document.getString("letter"));

		local.rowFromDocument = variables.cache.fromRediSearchDocument(local.document);

//		debug(local.row);
//		debug(local.rowFromDocument);
		assertEquals(local.row, local.rowFromDocument);
	}

}