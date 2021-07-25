component accessors = "true" extends = "lib.sql.QueryableCache" implements = "lib.sql.IWritable" {

	property name = "importBatchSize" type = "numeric" default = "1000";
	property name = "language" type = "string" default = "english";
	property name = "maxResults" type = "numeric" default = "1000";
	property name = "minPrefixLength" type = "numeric" default = "2";
	property name = "name" type = "string" setter = "false";

	// https://oss.redislabs.com/redisearch/Stopwords.html
	variables.DEFAULT_STOP_WORDS = "a is the an and are as at be but by for if in into it no not of on or such that their then there these they this to was will with";

	lib.redis.QueryableCache function init(required lib.redis.ConnectionPool connectionPool, required string name) {
		variables.connectionPool = arguments.connectionPool;
		variables.name = lCase(arguments.name);
		variables.normalizer = createObject("java", "java.text.Normalizer");
		variables.normalizerForm = createObject("java", "java.text.Normalizer$Form").NFD;
		variables.stopWords = "DEFAULT_STOP_WORDS";

		return this;
	}

	boolean function containsRow() {
		local.document = getClient().getDocument(getRowKey(argumentCollection = arguments));

		return structKeyExists(local, "document");
	}

	void function createIndex() {
		local.schema = createObject("java", "io.redisearch.Schema");

		for(local.field in getQueryable().getFieldList()) {
			if(getQueryable().fieldIsFilterable(local.field) || local.field == getIdentifierField()) {
				if(isRedisNumeric(local.field)) {
					local.schema.addSortableNumericField(local.field);
				} else {
					local.schema.addSortableTextField("_NFD_" & local.field, javaCast("double", 1));
				}
			}
		}

		local.indexOptions = createObject("java", "io.redisearch.client.Client$IndexOptions");
		local.indexOptions.init(local.indexOptions.KEEP_FIELD_FLAGS + local.indexOptions.USE_TERM_OFFSETS);

		if(len(variables.stopWords) == 0) {
			local.indexOptions.setNoStopwords();
		} else if(variables.stopWords == "DEFAULT_STOP_WORDS") {
			// use the underlying default within redisearch, dont override in the index
			variables.stopWords = variables.DEFAULT_STOP_WORDS;
		} else {
			local.indexOptions.setStopwords(listToArray(variables.stopWords, " "));
		}

		try {
			local.indexDefinition = createObject("java", "io.redisearch.client.IndexDefinition")
				.setPrefixes([ getName() ]);

			local.indexOptions.setDefinition(local.indexDefinition);
		} catch(Object e) {
			// IndexDefinition isn't included in the version of JRediSearch being used
		}

		getClient().createIndex(
			local.schema,
			local.indexOptions
		);
	}

	lib.sql.DeleteStatement function delete() {
		return new lib.sql.DeleteStatement(this);
	}

	void function dropIndex() {
		getClient().dropIndex(true);
	}

	void function executeDelete(required lib.sql.DeleteStatement deleteStatement) {
		if(len(arguments.deleteStatement.getWhere()) == 0) {
			dropIndex(true);
			createIndex();
		} else {
			do {
				local.targetRecords = this.select(getIdentifierField()).where(arguments.deleteStatement.getWhere()).execute();
				local.keys = [];

				for(local.row in local.targetRecords) {
					arrayAppend(local.keys, getRowKey(argumentCollection = local.row));
				}

				if(arrayLen(local.keys)) {
					getClient().deleteDocuments(true, local.keys);
				}
			} while(local.targetRecords.recordCount > 0);
		}
	}

	void function executeInsert(required lib.sql.InsertStatement insertStatement) {
		throw(type = "lib.redis.UnsupportedOperationException", message = "Insert is not supported in this implementation");
	}

	query function executeSelect(required lib.sql.SelectStatement selectStatement, required numeric limit, required numeric offset) {
		// not supported yet...
		if(arrayLen(arguments.selectStatement.getAggregates()) > 0) {
			throw(type = "lib.redis.UnsupportedOperationException", message = "Aggregates are not supported in this implementation");
		}

		// https://oss.redislabs.com/redisearch/Query_Syntax.html
		local.queryString = arguments.selectStatement.getWhereSQL()
			.REReplaceNoCase("^WHERE \(|\)$", "", "all")
			.trim();

		if(len(local.queryString) == 0) {
			local.queryString = "*";
		} else {
			local.criteria = arguments.selectStatement.getWhereCriteria();
			local.parameters = arguments.selectStatement.getParameters();

			for(local.i = 1; local.i <= arrayLen(local.criteria); local.i++) {
				// reset this on every iteration
				structDelete(local, "clause");
				structDelete(local, "value");

				if(isRedisNumeric(local.criteria[local.i].field)) {
					local.clause = "@#local.criteria[local.i].field#:";

					if(getQueryable().getFieldSQLType(local.criteria[local.i].field) == "bit") {
						local.value = local.parameters[local.i].value ? 1 : 0;
					} else if(isRedisDate(local.criteria[local.i].field)) {
						local.value = parseDateTime(local.parameters[local.i].value).getTime();
					}

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
							local.clause = listReduce(
								local.value,
								function(result, item) {
									return listAppend(arguments.result, " " & clause & "[#arguments.item# #arguments.item#] ", "|");
								},
								"",
								chr(31)
							);

							local.clause = "(" & trim(local.clause) & ")";

							if(local.criteria[local.i].operator == "NOT IN") {
								local.clause = "(-" & local.clause & ")";
							}
							break;
						case "=":
							local.clause &= "[#local.value# #local.value#]";
							break;
						default:
							throw(type = "lib.redis.UnsupportedOperatorException", message = "#local.criteria[local.i].operator# is not supported for this field (#local.criteria[local.i].field#)");
							break;
					}
				} else {
					local.clause = "@_NFD_#local.criteria[local.i].field#:";

					local.value = normalize(local.parameters[local.i].value);

					switch(local.criteria[local.i].operator) {
						case "LIKE":
							// fuzzy operate our normalized values
							local.value = listReduce(
								local.value,
								function(result, item) {
									if(len(arguments.item) >= getMinPrefixLength()) {
										arguments.result = listAppend(arguments.result, clause & arguments.item & "*", " ");
									}

									return arguments.result;
								},
								"",
								" "
							);

							// our criteria is no longer valid
							if(local.value == "") {
								local.searchException = true;
							}

							local.clause = "(" & local.value & ")";
							break;
						case "!=":
							local.clause = "-" & local.clause & '"' & local.value & '"';
							break;
						case "IN":
						case "NOT IN":
							local.clause = local.clause
								& "("
								& listReduce(
									local.value,
									function(result, item) {
										if(find(" ", arguments.item)) {
											arguments.item = '"' & arguments.item & '"';
										}

										return listAppend(arguments.result, arguments.item, "|");
									},
									"",
									chr(31)
								)
								& ")";

							if(local.criteria[local.i].operator == "NOT IN") {
								local.clause = "(-" & local.clause & ")";
							}
							break;
						case "=":
							local.clause &= '"' & local.value & '"';
							break;
						default:
							throw(type = "lib.redis.UnsupportedOperatorException", message = "#local.criteria[local.i].operator# is not supported for this field (#local.criteria[local.i].field#)");
							break;
					}
				}

				// replace our placeholders w/ the cache-friendly values
				local.queryString = replace(local.queryString, local.criteria[local.i].statement, "(" & local.clause & ")", "one");
			}
		}

		if(!structKeyExists(local, "searchException")) {
			try {
				local.queryString = local.queryString
					.replace(" AND ", " ", "all")
					.replace(" OR ", " | ", "all")
					.trim();

				if(arrayLen(arguments.selectStatement.getOrderCriteria()) > 1) {
					// why aggregation? redisearch only supports multiple sorts via the `aggregate` command
					local.ab = createObject("java", "io.redisearch.aggregation.AggregationBuilder")
						.init("'" & local.queryString & "'")
						.apply(variables.aggregateApply, "id");

					// sort
					local.sortedField = createObject("java", "io.redisearch.aggregation.SortedField");

					local.sortFields = arrayReduce(
						arguments.selectStatement.getOrderCriteria(),
						function(result, item) {
							local.f = listFirst(arguments.item, " ");

							if(isRedisNumeric(local.f)) {
								local.f = "@" & local.f;
							} else {
								local.f = "@_NFD_" & local.f;
							}

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

					// convert the command to a string for debugging purposes
					if(structKeyExists(arguments, "debug") && arguments.debug == "query") {
						local.byteArray = [ createObject("java", "java.lang.String").init(javaCast("string", "")).getBytes() ];
						local.ab.serializeRedisArgs(local.byteArray);
						local.string = arrayReduce(
							local.byteArray,
							function(result, item) {
								return listAppend(arguments.result, charsetEncode(arguments.item, "UTF-8"), " ");
							},
							""
						);

						throw(type = "lib.redis.DebugException", message = "ft.aggregate " & getName() & " " & local.string);
					}

					local.docIDs = [];
					local.ids = getClient().aggregate(local.ab);
					local.resultsLength = local.ids.totalResults;

					for(local.i = 0; local.i < local.ids.getResults().size(); local.i++) {
						arrayAppend(
							local.docIDs,
							// GUID/UUID get their dashes stripped during indexing
							replace(local.ids.getRow(javaCast("int", local.i)).getString("id"), " ", "", "all")
						);
					}

					local.documents = getClient().getDocuments(local.docIDs);
				} else {
					local.q = createObject("java", "io.redisearch.Query")
						.init("'" & local.queryString & "'")
						.returnFields(listToArray(arguments.selectStatement.getSelect()));

					if(arrayLen(arguments.selectStatement.getOrderCriteria()) == 0) {
						local.q.setSortBy(getIdentifierField(), true);
					} else {
						local.q.setSortBy(
							listFirst(arguments.selectStatement.getOrderBy(), " "),
							find("ASC", arguments.selectStatement.getOrderBy())
						);
					}

					if(arguments.limit > 0) {
						local.q.limit(arguments.offset, arguments.limit);
					} else {
						local.q.limit(0, getMaxResults());
					}

					local.results = getClient().search(local.q);
					local.resultsLength = local.results.totalResults;
					local.documents = local.results.docs;
				}
			} catch(redis.clients.jedis.exceptions.JedisDataException e) {
				// nada
				local.searchException = true;
			}
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

		if(structKeyExists(local, "documents") && local.documents.size() > 0) {
			for(local.i = 0; local.i < local.documents.size(); local.i++) {
				local.document = local.documents.get(local.i);

				if(!isNull(local.document)) {
					queryAddRow(
						local.query,
						fromRediSearchDocument(local.document, arguments.selectStatement.getSelect())
					);
				}
			}
		}

		local.query
			.getMetadata()
				.setExtendedMetadata({
					cached: !structKeyExists(local, "searchException"),
					engine: "redis",
					recordCount: structKeyExists(local, "documents") ? local.documents.size() : 0,
					totalRecordCount: (structKeyExists(local, "resultsLength") ? local.resultsLength : 0)
				});

		return local.query;
	}

	void function executeUpdate(required lib.sql.UpdateStatement updateStatement) {
		throw(type = "lib.redis.UnsupportedOperationException", message = "Update is not supported in this implementation");
	}

	void function executeUpsert(required lib.sql.UpsertStatement upsertStatement) {
		throw(type = "lib.redis.UnsupportedOperationException", message = "Upsert is not supported in this implementation");
	}

	struct function fromRediSearchDocument(required any document, string fieldFilter = "") {
		if(arguments.fieldFilter == "") {
			arguments.fieldFilter = getFieldList();
		}

		// backfill any missing values w/ reasonable defaults for data type
		return listReduce(
			arguments.fieldFilter,
			function(result, item) {
				if(document.hasProperty(arguments.item)) {
					local.value = document.get(arguments.item);
					if(!structKeyExists(local, "value")) {
						local.value = "";
					}

					if(isRedisDate(arguments.item) && isNumeric(val(local.value))) {
						arguments.result[arguments.item] = createObject("java", "java.util.Date").init(javaCast("long", val(local.value)));
					} else if(isRedisNumeric(arguments.item) && isNumeric(val(local.value))) {
						if(listFindNoCase("bigint,bit,integer,smallint,tinyint", getQueryable().getFieldSQLType(arguments.item))) {
							arguments.result[arguments.item] = val(local.value);
						} else {
							arguments.result[arguments.item] = javaCast("double", local.value);
						}
					} else if(len(local.value) > 0) {
						arguments.result[arguments.item] = local.value;
					}
				}

				if(!structKeyExists(arguments.result, arguments.item)) {
					arguments.result[arguments.item] = javaCast("null", "");
				}

				return arguments.result;
			},
			{}
		);

		return {};
	}

	private any function getClient() {
		return createObject("java", "io.redisearch.client.Client").init(variables.name, variables.connectionPool.getJedisPool());
	}

	struct function getInfo() {
		return getClient().getInfo();
	}

	any function getRow() {
		local.document = getClient().getDocument(getRowKey(argumentCollection = arguments));

		if(!isNull(local.document)) {
			local.result = fromRediSearchDocument(local.document);

			for(local.key in getFieldList()) {
				if(!structKeyExists(local.result, local.key)) {
					local.result[local.key] = "";
				}
			}

			return local.result;
		}
	}

	lib.sql.InsertStatement function insert(required struct fields) {
		throw(type = "lib.redis.UnsupportedOperationException", message = "Insert is not supported in this implementation");
	}

	boolean function isRedisDate(required string field) {
		switch(getQueryable().getFieldSQLType(arguments.field)) {
			case "date":
			case "time":
			case "timestamp":
				return true;
				break;
		};

		return false;
	}

	boolean function isRedisNumeric(required string field) {
		switch(getQueryable().getFieldSQLType(arguments.field)) {
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
				return true;
				break;
		};

		return false;
	}

	private string function normalize(required string input) {
		arguments.input = lCase(arguments.input);

		// https://oss.redislabs.com/redisearch/Escaping.html
		arguments.input = listReduce(
			variables.normalizer.normalize(javaCast("string", arguments.input), variables.normalizerForm).replaceAll("\p{InCombiningDiacriticalMarks}+", ""),
			function(outerResult, outerItem) {
				arguments.outerItem = listReduce(
					trim(REReplace(arguments.outerItem, "\W+", " ", "all")),
					function(innerResult, innerItem) {
						// these values have all been lCase'd at this point
						if(!listFind(variables.stopWords, arguments.innerItem, " ")) {
							arguments.innerResult = listAppend(arguments.innerResult, arguments.innerItem, " ");
						}

						return arguments.innerResult;
					},
					"",
					" "
				);

				return listAppend(arguments.outerResult, arguments.outerItem, chr(31));
			},
			"",
			chr(31)
		);

		return trim(arguments.input);
	}

	void function putRow(required struct row) {
		getClient().addDocument(
			toRediSearchDocument(arguments.row),
			createObject("java", "io.redisearch.client.AddOptions")
				.setLanguage(getLanguage())
				.setReplacementPolicy(createObject("java", "io.redisearch.client.AddOptions$ReplacementPolicy").FULL)
		);
	}

	void function removeRow() {
		getClient().deleteDocument(javaCast("string", getRowKey(argumentCollection = arguments)), true);
	}

	void function seedFromQueryable(boolean overwrite = false, string where = "") {
		local.documents = [];
		local.replacementPolicy = createObject("java", "io.redisearch.client.AddOptions$ReplacementPolicy");
		local.addOptions = createObject("java", "io.redisearch.client.AddOptions")
			.setLanguage(getLanguage())
			.setReplacementPolicy(arguments.overwrite ? local.replacementPolicy.FULL : local.replacementPolicy.NONE);

		for(local.row in getQueryable().select().where(arguments.where).execute()) {
			arrayAppend(local.documents, toRediSearchDocument(local.row));

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

	lib.sql.QueryableCache function setRowKeyMask(required string rowKeyMask) {
		super.setRowKeyMask(argumentCollection = arguments);

		local.fl = getFieldList();

		// parse the rowKeyMask to determine what properties we need
		variables.aggregateApply = arrayReduce(
			getKeyFields(),
			function(result, field) {
				return listAppend(replace(arguments.result, "{#lCase(arguments.field)#}", "%s"), (isRedisNumeric(arguments.field) ? "@" : "@_NFD_") & listGetAt(fl, listFindNoCase(fl, arguments.field)));
			},
			"'#getRowKeyMask()#'"
		);

		variables.aggregateApply = "format(#variables.aggregateApply#)";

		return this;
	}

	lib.sql.QueryableCache function setStopWords(required string stopWords) {
		if(structKeyExists(variables, "queryable")) {
			throw(type = "lib.redis.QueryableDefinedException", message = "an IQueryable has been furnished already");
		}

		variables.stopWords = arguments.stopWords;

		return this;
	}

	any function toRediSearchDocument(required struct row) {
		if(!structKeyExists(arguments.row, getIdentifierField())) {
			throw(type = "lib.redis.MissingIdentifierException", message = "the identifier field #getIdentifierField()# must be provided");
		}

		local.document = createObject("java", "io.redisearch.Document").init(getRowKey(argumentCollection = arguments.row));

		for(local.field in getQueryable().getFieldList()) {
			if(!structKeyExists(arguments.row, local.field)) {
				arguments.row[local.field] = "";
			}

			if(isRedisDate(local.field)) {
				if(isDate(arguments.row[local.field])) {
					local.document.set(local.field, javaCast("long", arguments.row[local.field].getTime()));
				}
			} else if(isRedisNumeric(local.field)) {
				if(isNumeric(arguments.row[local.field]) || isBoolean(arguments.row[local.field])) {
					if(listFindNoCase("bigint,bit,integer,smallint,tinyint", getQueryable().getFieldSQLType(local.field))) {
						local.document.set(local.field, javaCast("long", arguments.row[local.field]));
					} else {
						local.document.set(local.field, javaCast("double", arguments.row[local.field]));
					}
				}
			} else {
				local.document.set(local.field, javaCast("string", arguments.row[local.field]));

				if(getQueryable().fieldIsFilterable(local.field) || local.field == getIdentifierField()) {
					local.document.set("_NFD_" & local.field, javaCast("string", normalize(arguments.row[local.field])));
				}
			}
		}

		return local.document;
	}

	lib.sql.UpdateStatement function update(required struct fields) {
		throw(type = "lib.redis.UnsupportedOperationException", message = "Update is not supported in this implementation");
	}

	lib.sql.UpsertStatement function upsert(required struct fields) {
		throw(type = "lib.redis.UnsupportedOperationException", message = "Upsert is not supported in this implementation");
	}

}