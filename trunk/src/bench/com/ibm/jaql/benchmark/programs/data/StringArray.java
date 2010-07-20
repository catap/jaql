package com.ibm.jaql.benchmark.programs.data;

import com.ibm.jaql.benchmark.JsonConverter;
import com.ibm.jaql.benchmark.SchemaDescription;
import com.ibm.jaql.json.schema.ArraySchema;
import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.type.JsonArray;
import com.ibm.jaql.json.type.JsonValue;

public class StringArray implements JsonConverter, SchemaDescription {

	@Override
	public Object convert(JsonValue val) {
		JsonArray arr = (JsonArray)val;
		JsonValue[] values = new JsonValue[(int) arr.count()];
		try {
			arr.getAll(values);
		} catch (Exception e) {
			throw new RuntimeException("Bbbbbbbb");
		}
		
		String[] strings = new String[values.length];
		for (int i = 0; i < strings.length; i++) {
			strings[i] = values[i].toString();
		}
		
		return strings;
	}

	@Override
	public Schema getSchema() {
		return new ArraySchema(null, SchemaFactory.stringSchema());
	}

}
