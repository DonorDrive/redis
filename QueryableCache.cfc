component accessors = "true" extends = "lib.sql.QueryableCache" {

	property name = "importBatchSize" type = "numeric" default = "1000";
	property name = "name" type = "string" setter = "false";
	property name = "maxResults" type = "numeric" default = "1000";

	lib.redis.QueryableCache function init(required lib.redis.ConnectionPool connectionPool, required string name) {
		variables.connectionPool = arguments.connectionPool;
		variables.name = lCase(arguments.name);

		return this;
	}

	boolean function containsRow() {
		local.document = getClient().getDocument(getRowKey(argumentCollection = arguments));

		return structKeyExists(local, "document");
	}

	void function createIndex() {
		local.schema = new java("io.redisearch.Schema");

		for(local.field in getQueryable().getFieldList()) {
			if(getQueryable().fieldIsFilterable(local.field)) {
				switch(getQueryable().getFieldSQLType(local.field)) {
					case "bigint":
					case "bit":
					case "date":
					case "decimal":
					case "double":
					case "float":
					case "integer":
					case "money":
					case "numeric":
					case "real":
					case "smallint":
					case "time":
					case "timestamp":
					case "tinyint":
						local.schema.addSortableNumericField(local.field);
						break;
					default:
						local.schema.addSortableTextField(local.field, javaCast("double", 1));
						break;
				};
			}
		}

		getClient().createIndex(
			local.schema,
			new java("io.redisearch.client.Client$IndexOptions").init(0)
		);
	}

	void function dropIndex() {
		getClient().dropIndex();
	}

	query function executeSelect(required lib.sql.SelectStatement selectStatement, required numeric limit, required numeric offset) {
		// not yet...
		if(arrayLen(arguments.selectStatement.getAggregates()) > 0) {
			throw(type = "lib.redis.UnsupportedOperationException", message = "Aggregates are not supported in this implementation");
		}

		local.queryString = arguments.selectStatement.getWhereSQL()
			.REReplaceNoCase("^WHERE \(|\)$", "", "all")
			.trim();

		if(len(local.queryString) == 0) {
			local.queryString = "*";
		} else {
			local.criteria = arguments.selectStatement.getWhereCriteria();
			local.parameters = arguments.selectStatement.getParameters();

			for(local.i = 1; local.i <= arrayLen(local.criteria); local.i++) {
				local.clause = "@#local.criteria[local.i].field#:";
				// reset this on every iteration
				structDelete(local, "value");

				switch(local.parameters[local.i].cfsqltype) {
					// no break? all numeric fields get treated the same, save for some minor formatting considerations
					case "bit":
						if(!structKeyExists(local, "value")) {
							local.value = local.parameters[local.i].value ? 1 : 0;
						}
					case "date":
					case "time":
					case "timestamp":
						if(!structKeyExists(local, "value")) {
							local.value = parseDateTime(local.parameters[local.i].value).getTime();
						}
					case "bigint":
					case "decimal":
					case "double":
					case "float":
					case "integer":
					case "money":
					case "numeric":
					case "real":
					case "smallint":
					case "tinyint":
						if(!structKeyExists(local, "value")) {
							local.value = local.parameters[local.i].value;
						}

						switch(local.criteria[local.i].operator) {
							case "!=":
								local.clause = "-" & local.clause & "[#local.value# #local.value#]";
								break;
							case ">=":
								local.clause &= "[#local.value# +inf]";
								break;
							case ">":
								local.clause &= "[(#local.value# +inf]";
								break;
							case "<=":
								local.clause &= "[-inf #local.value#]";
								break;
							case "<":
								local.clause &= "[-inf (#local.value#]";
								break;
							case "IN":
							case "NOT IN":
								local.clause = "(" & listReduce(
									local.value,
									function(result, item) {
										return listAppend(arguments.result, clause & "[#arguments.item# #arguments.item#]", "|");
									},
									"",
									chr(31)
								) & ")";

								if(local.criteria[local.i].operator == "NOT IN") {
									local.clause = "(-" & local.clause & ")";
								}
								break;
							default:
								local.clause &= "[#local.value# #local.value#]";
								break;
						}
						break;
					default:
						local.value = local.parameters[local.i].value
							// remove wildcard operators - will re-add where appropriate
							.replace("%", " ", "all")
							// escape dashes
							.replace("-", "\-", "all")
							.trim();

						switch(local.criteria[local.i].operator) {
							case "LIKE":
								local.clause &= REReplace(local.value, "\s+|$", "*", "all");
								break;
							case "!=":
								local.clause = "-" & local.clause & '"' & local.value & '"';
								break;
							case "IN":
							case "NOT IN":
								local.clause &= "(" & replace('"' & local.value & '"', chr(31), '"|"', "all") & ")";

								if(local.criteria[local.i].operator == "NOT IN") {
									local.clause = "(-" & local.clause & ")";
								}
								break;
							default:
								local.clause &= '"' & local.value & '"';
								break;
						}
						break;
				};

				// replace our placeholders w/ the cache-friendly values
				local.queryString = replace(local.queryString, local.criteria[local.i].statement, local.clause, "one");
			}
		}

		local.docIDs = [];

		try {
			local.queryString = local.queryString
				.replace(" AND ", " ", "all")
				.replace(" OR ", " | ", "all")
				.trim();

			// why aggregation? redisearch only supports multiple sorts via the `aggregate` command
			local.ab = new java("io.redisearch.aggregation.AggregationBuilder")
				.init("'" & local.queryString & "'")
				.apply("@#getIdentifierField()#", "id");

			// sort
			local.sortedField = new java("io.redisearch.aggregation.SortedField");

			if(arrayLen(arguments.selectStatement.getOrderCriteria()) > 0) {
				local.sortFields = arrayReduce(
					arguments.selectStatement.getOrderCriteria(),
					function(result, item) {
						local.f = "@" & listFirst(arguments.item, " ");
						local.o = listLast(arguments.item, " ");

						if(local.o == "DESC") {
							arrayAppend(arguments.result, sortedField.desc(local.f));
						} else {
							arrayAppend(arguments.result, sortedField.asc(local.f));
						}

						return arguments.result;
					},
					[]
				);
			} else {
				local.sortFields = [ local.sortedField.asc("@" & getIdentifierField()) ];
			}

			local.maxResults = getMaxResults();

			if(arguments.limit > 0) {
				if(arguments.offset > 0) {
					local.maxResults = arguments.offset + arguments.limit;
				} else {
					local.maxResults = arguments.limit;
				}
			}

			local.ab.sortBy(javaCast("int", local.maxResults), local.sortFields);

			// pagination
			// look familiar? - this is the same logic as above... just not good way to re-use the condition, as the syntax must be assembled in order
			if(arguments.limit > 0) {
				if(arguments.offset > 0) {
					local.ab.limit(arguments.offset, arguments.limit);
				} else {
					local.ab.limit(arguments.limit);
				}
			}

			local.searchClient = getClient();

			// convert the command to a string for debugging purposes
			/*
			local.byteArray = [ new java("java.lang.String").init(JavaCast("string", "")).getBytes() ];
			local.ab.serializeRedisArgs(local.byteArray);
			local.string = arrayReduce(
				local.byteArray,
				function(result, item) {
					return arguments.result & " " & charsetEncode(arguments.item, "UTF-8");
				},
				""
			);
			throw(message = local.string);
			*/

			local.ids = local.searchClient.aggregate(local.ab);
			local.resultsLength = local.ids.totalResults;

			for(local.i = 0; local.i < local.ids.getResults().size(); local.i++) {
				arrayAppend(
					local.docIDs,
					// strip escape characters back out to get the ID
					getRowKey(argumentCollection = { "#getIdentifierField()#": replace(local.ids.getRow(javaCast("int", local.i)).getString("id"), "\-", "-", "all") })
				);
			}
		} catch(redis.clients.jedis.exceptions.JedisDataException e) {
			// nada
			local.cacheException = true;
		}

		// queryNew only supports a subset of the data types that queryparam does
		local.fieldSQLTypes = listReduce(
			arguments.selectStatement.getSelect(),
			function(v, i) {
				switch(getFieldSQLType(i)) {
					case "char":
						return listAppend(arguments.v, "varchar");
						break;
					case "float":
					case "money":
					case "numeric":
					case "real":
						return listAppend(arguments.v, "double");
						break;
					case "smallint":
					case "tinyint":
						return listAppend(arguments.v, "integer");
						break;
					default:
						return listAppend(arguments.v, getFieldSQLType(arguments.i));
						break;
				};
			},
			""
		);

		local.query = queryNew(arguments.selectStatement.getSelect(), local.fieldSQLTypes);

		if(arrayLen(local.docIDs) > 0) {
			local.documents = local.searchClient.getDocuments(local.docIDs);

			for(local.i = 0; local.i < local.documents.size(); local.i++) {
				local.document = local.documents.get(local.i);

				if(!isNull(local.document)) {
					queryAddRow(
						local.query,
						fromRedisearchDocument(local.document, arguments.selectStatement.getSelect())
					);
				}
			}
		}

		local.query
			.getMetadata()
				.setExtendedMetadata({
					cached: !structKeyExists(local, "cacheException"),
					engine: "redis",
					recordCount: arrayLen(local.docIDs),
					totalRecordCount: (structKeyExists(local, "resultsLength") ? local.resultsLength : 0)
				});

		return local.query;
	}

	struct function fromRedisearchDocument(required any document, string fieldFilter = "") {
		if(arguments.fieldFilter == "") {
			arguments.fieldFilter = getFieldList();
		}

		// backfill any missing values w/ reasonable defaults for data type
		return listReduce(
			arguments.fieldFilter,
			function(result, item) {
				switch(getQueryable().getFieldSQLType(arguments.item)) {
					case "bigint":
					case "bit":
					case "decimal":
					case "double":
					case "float":
					case "integer":
					case "money":
					case "numeric":
					case "real":
					case "smallint":
					case "tinyint":
						if(document.hasProperty(arguments.item)) {
							arguments.result[arguments.item] = val(document.getString(arguments.item));
						} else {
							arguments.result[arguments.item] = javaCast("null", "");
						}
						break;
					case "date":
					case "time":
					case "timestamp":
						if(document.hasProperty(arguments.item)) {
							arguments.result[arguments.item] = new java("java.util.Date").init(javaCast("long", document.getString(arguments.item)));
						} else {
							arguments.result[arguments.item] = javaCast("null", "");
						}
						break;
					default:
						if(document.hasProperty(arguments.item)) {
							arguments.result[arguments.item] = document.getString(arguments.item).replace("\-", "-", "all");
						} else {
							arguments.result[arguments.item] = javaCast("null", "");
						}
						break;
				};

				return arguments.result;
			},
			{}
		);

		return {};
	}

	private any function getClient() {
		return new java("io.redisearch.client.Client").init(variables.name, variables.connectionPool.getJedisPool());
	}

	any function getRow() {
		local.document = getClient().getDocument(getRowKey(argumentCollection = arguments));

		if(!isNull(local.document)) {
			return fromRedisearchDocument(local.document);
		}
	}

	void function putRow(required struct row) {
		getClient().addDocument(
			toRedisearchDocument(arguments.row),
			new java("io.redisearch.client.AddOptions")
				.setLanguage("english")
				.setReplacementPolicy(new java("io.redisearch.client.AddOptions$ReplacementPolicy").FULL)
		);
	}

	void function removeRow() {
		getClient().deleteDocument(javaCast("string", getRowKey(argumentCollection = arguments)), true);
	}

	void function seedFromQueryable(boolean overwrite = false) {
		local.documents = [];
		local.replacementPolicy = new java("io.redisearch.client.AddOptions$ReplacementPolicy");
		local.addOptions = new java("io.redisearch.client.AddOptions")
			.setLanguage("english")
			.setReplacementPolicy(arguments.overwrite ? local.replacementPolicy.FULL : local.replacementPolicy.NONE);

		for(local.row in getQueryable().select().execute()) {
			arrayAppend(local.documents, toRedisearchDocument(local.row));

			if(arrayLen(local.documents) == getImportBatchSize()) {
				getClient().addDocuments(local.addOptions, local.documents);
				local.documents = [];
			}
		}

		if(arrayLen(local.documents) > 0) {
			getClient().addDocuments(local.addOptions, local.documents);
		}
	}

	lib.sql.QueryableCache function setQueryable(required lib.sql.IQueryable queryable) {
		if(structKeyExists(variables, "queryable")) {
			throw(type = "lib.redis.QueryableDefinedException", message = "an IQueryable has been furnished already");
		}

		super.setQueryable(arguments.queryable);

		if(!structKeyExists(variables, "rowKeyMask")) {
			setRowKeyMask(lCase(getName() & ":" & getIdentifierField() & ":{" & getIdentifierField() & "}"));
		}

		try{
			// create the index
			createIndex();
		} catch(redis.clients.jedis.exceptions.JedisDataException e) {
			// index exists already
		}

		return this;
	}

	any function toRedisearchDocument(required struct row) {
		if(!structKeyExists(arguments.row, getIdentifierField())) {
			throw(type = "lib.redis.MissingIdentifierException", message = "the identifier field #getIdentifierField()# must be provided");
		}

		local.fields = new java("java.util.HashMap").init();

		for(local.field in getQueryable().getFieldList()) {
			switch(getQueryable().getFieldSQLType(local.field)) {
				case "bigint":
					if(structKeyExists(arguments.row, local.field) && isNumeric(arguments.row[local.field])) {
						local.fields.put(local.field, javaCast("long", arguments.row[local.field]));
					}
					break;
				case "bit":
					if(structKeyExists(arguments.row, local.field) && isBoolean(arguments.row[local.field])) {
						local.fields.put(local.field, javaCast("int", arguments.row[local.field]));
					}
					break;
				case "date":
				case "time":
				case "timestamp":
					if(structKeyExists(arguments.row, local.field) && isDate(arguments.row[local.field])) {
						local.fields.put(local.field, javaCast("long", arguments.row[local.field].getTime()));
					}
					break;
				case "decimal":
				case "double":
				case "money":
				case "numeric":
					if(structKeyExists(arguments.row, local.field) && isNumeric(arguments.row[local.field])) {
						local.fields.put(local.field, javaCast("double", arguments.row[local.field]));
					}
					break;
				case "float":
				case "real":
					if(structKeyExists(arguments.row, local.field) && isNumeric(arguments.row[local.field])) {
						local.fields.put(local.field, javaCast("float", arguments.row[local.field]));
					}
					break;
				case "integer":
				case "smallint":
				case "tinyint":
					if(structKeyExists(arguments.row, local.field) && isNumeric(arguments.row[local.field])) {
						local.fields.put(local.field, javaCast("int", arguments.row[local.field]));
					}
					break;
				default:
					if(structKeyExists(arguments.row, local.field) && len(arguments.row[local.field]) > 0) {
						// escape dashes for guids/uuids
						local.fields.put(local.field, javaCast("string", replace(arguments.row[local.field], "-", "\-", "all")));
					}
					break;
			};
		}

		return new java("io.redisearch.Document").init(
			getRowKey(argumentCollection = arguments.row),
			local.fields,
			javaCast("double", 1)
		);
	}

}