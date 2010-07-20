package com.ibm.jaql.benchmark.programs.data;

import com.ibm.jaql.benchmark.JsonConverter;
import com.ibm.jaql.benchmark.SchemaDescription;
import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.type.JsonValue;

public class Null implements JsonConverter, SchemaDescription {

	@Override
	public Object convert(JsonValue val) {
		return null;
	}

	@Override
	public Schema getSchema() {
		return SchemaFactory.nullSchema();
	}

}
