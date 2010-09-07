package com.ibm.jaql.benchmark.programs;

import com.ibm.jaql.benchmark.JsonBenchmarkProgramSingleInput;
import com.ibm.jaql.json.type.BufferedJsonArray;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;

public class JsonKeyExtract extends JsonBenchmarkProgramSingleInput {
	private final JsonString keyField = new JsonString("forename");
	BufferedJsonArray arr = new BufferedJsonArray(2);
	JsonValue[] vals = new JsonValue[2];

	@Override
	public JsonValue nextResult(JsonValue val) {
		JsonRecord rec = (JsonRecord) val;
		vals[0] = rec.get(keyField);
		vals[1] = rec;
		arr.set(vals, 2);
		return arr;
	}

}
