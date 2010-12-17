package com.ibm.jaql.benchmark.programs.data;

import com.ibm.jaql.benchmark.JsonConverter;
import com.ibm.jaql.benchmark.SchemaDescription;
import com.ibm.jaql.json.schema.ArraySchema;
import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.type.JsonArray;
import com.ibm.jaql.json.type.JsonNumber;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;

public class PersonArrayCompact implements JsonConverter, SchemaDescription {

	@Override
	public Object convert(JsonValue val) {
		JsonArray arr = (JsonArray) val;
		JsonValue[] values = new JsonValue[2];
		try {
			arr.getAll(values);
		} catch (Exception e) {
			return new RuntimeException("Hilfe!");
		}
		
		Object[] os = new Object[3];
		os[1] = ((JsonNumber)values[2]).intValue();
		os[0] = ((JsonString)values[0]).toString();
		
		return os;
	}

	@Override
	public Schema getSchema() {
		return new ArraySchema(new Schema[] {SchemaFactory.stringSchema(), SchemaFactory.numberSchema()});
	}

}
