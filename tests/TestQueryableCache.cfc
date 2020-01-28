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
				"id, createdTimestamp, createdDate, createdTime, foo, bar, letter",
				"integer, timestamp, date, time, varchar, bit, varchar"
			);

			queryAddRow(
				variables.query,
				[
					{
						"id": 1,
						"bar": true,
						"foo": "9E835384-07E9-724C-1911F370FCFA67A6",
						"createdTimestamp": parseDateTime("2019-12-12T00:00:00.123Z"),
						"createdDate": createDate(2019, 12, 12),
						"createdTime": createTime(0, 0, 0),
						"letter": "A"
					},
					{
						"id": 2,
						"bar": true,
						"foo": "ECDC79F4-A719-D461-81BEC1A394697213",
						"createdTimestamp": parseDateTime("2019-12-25T00:00:00.001Z"),
						"createdDate": createDate(2019, 12, 25),
						"createdTime": createTime(0, 0, 0),
						"letter": "B"
					},
					{
						"id": 3,
						"bar": true,
						"foo": "EFA8C662-B0A9-FD1D-99FB38E815EDA2C5",
						"createdTimestamp": parseDateTime("2019-12-31T00:00:00.000Z"),
						"createdDate": createDate(2019, 12, 31),
						"createdTime": createTime(0, 0, 0),
						"letter": "C"
					}
				]
			);

			variables.now = now();
			for(local.i = 4; local.i <= 1000; local.i++) {
				queryAddRow(
					variables.query,
					{
						"id": local.i,
						"bar": (!randRange(1, 3) % 2 ? local.i % 2 : javaCast("null", "")),
						"foo": createUUID(),
						"createdTimestamp": (!randRange(1, 3) % 2 ? dateAdd("s", -(local.i), variables.now) : javaCast("null", "")),
						"createdDate": (!randRange(1, 3) % 2 ? variables.now : javaCast("null", "")),
						"createdTime": (!randRange(1, 3) % 2 ? variables.now : javaCast("null", "")),
						"letter": chr(64 + randRange(1, 25) + (local.i % 2 ? 32 : 0))
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
		// indexes are prefixed w/ idx:
		local.queryableCache = new lib.redis.Cache(variables.connectionPool, "idx");

		try{
			// create the index
			variables.cache.createIndex();
		} catch(redis.clients.jedis.exceptions.JedisDataException e) {
			// index exists already, do nothing
		}

		assertTrue(local.queryableCache.containsKey("mxunit:indexed"));
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
		assertEquals("mxunit:indexed:id:1", variables.cache.getRowKey(id = 1));
	}

	function test_putRow_getRow() {
		local.compareDate = createDateTime(1948, 12, 10, 10, 0, 0);
		local.queryRow = queryGetRow(variables.query, 1);
		local.queryRow.createdTimestamp = local.compareDate;
		variables.cache.putRow(local.queryRow);
		local.compare = variables.cache.getRow(argumentCollection = local.queryRow);

		assertEquals(0, dateCompare(local.compareDate, local.compare.createdTimestamp));
	}

	function test_removeRow() {
		local.queryRow = queryGetRow(variables.query, 1);
		local.queryRow.id = createUUID();
		variables.cache.putRow(local.queryRow);

		assertTrue(variables.cache.containsRow(id = local.queryRow.id));
		variables.cache.removeRow(id = local.queryRow.id);
		assertFalse(variables.cache.containsRow(id = local.queryRow.id));
	}

	function test_seedFromQueryable_overwrite() {
		variables.cache.seedFromQueryable();

		local.row = queryGetRow(variables.query, 2);
		local.row.foo = createUUID();

		variables.cache.putRow(local.row);

//		debug(local.row);

		variables.cache.seedFromQueryable(overwrite = true);

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
		assertEquals("1000,999,998,997,996,995,994,993,992,991", valueList(local.result.id));
	}

	function test_select_orderBy_limit_offset() {
		local.result = variables.cache.select().orderBy("id ASC").execute(limit = 10, offset = 10);

		debug(local.result);
		assertEquals(10, local.result.recordCount);
		assertEquals(1000, local.result.getMetadata().getExtendedMetadata().totalRecordCount);
		assertEquals("11,12,13,14,15,16,17,18,19,20", valueList(local.result.id));
	}

	function test_select_where() {
		local.result = variables.cache.select().where("id = #variables.query.id#").execute();

		debug(local.result);
		assertEquals(variables.query.id, local.result.id);
		assertEquals(variables.query.createdTimestamp, local.result.createdTimestamp);
		assertEquals(variables.query.createdDate, local.result.createdDate);
		assertEquals(variables.query.createdTime, local.result.createdTime);
		assertEquals(1, local.result.recordCount);
	}

	function test_select_where_compound_limit() {
		local.result = variables.cache.select().where("id < 100 AND createdTimestamp >= '#dateTimeFormat(dateAdd("d", -1, variables.now), "yyyy-mm-dd HH:nn:ss.l")#'").execute(limit = 10);

//		debug(local.result);
		assertEquals(10, local.result.recordCount);
	}

	function test_select_where_DD_12824() {
		local.result = variables.cache.select().where("createdTimestamp < '#dateTimeFormat(now(), 'yyyy-mm-dd HH:nn:ss.l')#' AND bar = 1").execute();

		debug(local.result);
	}

	function test_select_where_DD_13660() {
		local.result = variables.cache.select("letter, id").where("id > 990.00").execute();

		debug(local.result);
	}

	function test_select_where_DD_13763() {
		local.where = "foo LIKE '%#listFirst(lCase(variables.query.foo[1]), '-')#%'";
		local.result = variables.cache.select().where(local.where).orderBy("id ASC").execute();

		assertTrue(local.result.recordCount >= 1);

		debug(local.where);
		debug(local.result);
	}

	function test_select_where_DDMAINT_20768() {
		local.where = "createdTimestamp >= '#dateTimeFormat(dateAdd("s", -10, variables.now), "yyyy-mm-dd HH:nn:ss")#'";

		local.assert = variables.queryable.select().where(local.where).orderBy("id ASC").execute();
		local.result = variables.cache.select().where(local.where).orderBy("id ASC").execute();

		debug(local.assert);
		debug(local.result);

		assertEquals(local.assert, local.result);
	}

	function test_select_where_in() {
		local.result = variables.cache.select("id, foo").where("foo IN ('#variables.query.foo[1]#', '#variables.query.foo[2]#', '#variables.query.foo[3]#') OR id IN (5, 10, 15)").execute();

//		debug(local.result);
		assertEquals("foo,id", listSort(local.result.columnList, "textnocase"));
		assertEquals("1,2,3,5,10,15", listSort(valueList(local.result.id), "numeric"));
		assertEquals(6, local.result.recordCount);

		local.result = variables.cache.select("id, foo").where("foo IN ('#variables.query.foo[1]#','#variables.query.foo[2]#')").execute();

//		debug(local.result);
		assertEquals("foo,id", listSort(local.result.columnList, "textnocase"));
		assertEquals("1,2", listSort(valueList(local.result.id), "numeric"));

		// test a single record
		local.result = variables.cache.select("id, foo").where("foo IN ('#variables.query.foo[1]#'").execute();

//		debug(local.result);
		assertEquals(1, local.result.recordCount);
	}

	function test_select_where_not_in() {
		// numeric filtering
		local.result = variables.cache.select("id, foo").where("id NOT IN (5, 10, 15) AND id < 15").execute();

//		debug(local.result);
		assertEquals("foo,id", listSort(local.result.columnList, "textnocase"));
		assertEquals("1,2,3,4,6,7,8,9,11,12,13,14", listSort(valueList(local.result.id), "numeric"));

		// string filtering
		local.result = variables.cache.select("id, foo").where("foo NOT IN ('#variables.query.foo[1]#', '#variables.query.foo[2]#', '#variables.query.foo[3]#') AND id < 15").execute();

		debug(local.result);
		assertEquals("foo,id", listSort(local.result.columnList, "textnocase"));
		assertEquals("4,5,6,7,8,9,10,11,12,13,14", listSort(valueList(local.result.id), "numeric"));

		// test negation of a single record
		local.result = variables.cache.select("id, foo").where("foo NOT IN ('#variables.query.foo[1]#'").execute();

//		debug(local.result);
		assertEquals("foo,id", listSort(local.result.columnList, "textnocase"));
		assertEquals(999, local.result.recordCount);
	}

	function test_select_where_orderBy_limit() {
		local.result = variables.cache.select().where("id <= 500").orderBy("id DESC").execute(limit = 10);

//		debug(local.result);
		assertEquals("500,499,498,497,496,495,494,493,492,491", valueList(local.result.id));
		assertEquals(10, local.result.recordCount);
	}

	function test_toRedisearchDocument_fromRedisearchDocument() {
		local.row = queryGetRow(variables.query, 1);

		local.document = variables.cache.toRedisearchDocument(local.row);

		assertEquals(local.row.letter, local.document.getString("letter"));

		local.rowFromDocument = variables.cache.fromRedisearchDocument(local.document);

//		debug(local.rowFromDocument);
		assertEquals(local.row, local.rowFromDocument);
	}

}