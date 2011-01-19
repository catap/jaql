package com.ibm.jaql.benchmark.programs;

import com.ibm.jaql.benchmark.JsonBenchmarkProgramSingleInput;
import com.ibm.jaql.json.type.BufferedJsonArray;
import com.ibm.jaql.json.type.JsonArray;
import com.ibm.jaql.json.type.JsonValue;

public class JsonProjectArray extends JsonBenchmarkProgramSingleInput {
	BufferedJsonArray arr;
	JsonValue[] values;
	JsonValue[] buffer;
	
	public JsonProjectArray() {
		arr = new BufferedJsonArray(2);
		values = new JsonValue[2];
		buffer = new JsonValue[3];
	}
	
	@Override
	public JsonValue nextResult(JsonValue val) {
		JsonArray person = (JsonArray) val;
		try {
			person.getAll(buffer);
		} catch (Exception e) {
			throw new RuntimeException("Test");
		}
		values[0] = buffer[0];
		values[1] = buffer[2];
		arr.set(values, 2);
		
		return arr;
	}
}
