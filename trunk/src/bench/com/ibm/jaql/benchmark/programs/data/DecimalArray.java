package com.ibm.jaql.benchmark.programs.data;

import com.ibm.jaql.benchmark.SchemaDescription;
import com.ibm.jaql.benchmark.JsonConverter;
import com.ibm.jaql.json.schema.ArraySchema;
import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.type.JsonArray;
import com.ibm.jaql.json.type.JsonNumber;
import com.ibm.jaql.json.type.JsonValue;

public class DecimalArray implements JsonConverter, SchemaDescription {

	@Override
	public Object convert(JsonValue val) {
		JsonArray arr = (JsonArray) val;
		JsonValue[] values = new JsonValue[(int) arr.count()];
		try {
			arr.getAll(values);
		} catch (Exception e) {
			return new RuntimeException("eeeee");
		}
		
		//TODO: Null when array is empty;
		int[] decimals = new int[values.length];
		for (int i = 0; i < decimals.length; i++) {
			decimals[i] = ((JsonNumber)values[i]).intValue();
		}
		
		return decimals;
	}

	@Override
	public Schema getSchema() {
		return new ArraySchema(null, SchemaFactory.longSchema());
	}
}
