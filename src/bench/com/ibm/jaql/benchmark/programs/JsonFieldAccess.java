package com.ibm.jaql.benchmark.programs;

import com.ibm.jaql.benchmark.JsonBenchmarkProgramSingleInput;
import com.ibm.jaql.json.type.BufferedJsonRecord;
import com.ibm.jaql.json.type.JsonDecimal;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.util.FieldNameCache;

public class JsonFieldAccess extends JsonBenchmarkProgramSingleInput {
	BufferedJsonRecord rec;
	public static final JsonString ID_FIELD = FieldNameCache.get(new JsonString("id"));
	
	public JsonFieldAccess() {
		rec = new BufferedJsonRecord();
		rec.add(ID_FIELD, new JsonDecimal(-1));
	}
	
	@Override
	public JsonValue nextResult(JsonValue val) {
		return ((JsonRecord)val).get(ID_FIELD);
	}
}
