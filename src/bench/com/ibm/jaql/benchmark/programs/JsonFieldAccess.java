package com.ibm.jaql.benchmark.programs;

import com.ibm.jaql.benchmark.JsonSingleDataBenchmarkProgram;
import com.ibm.jaql.json.type.BufferedJsonRecord;
import com.ibm.jaql.json.type.JsonDecimal;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;

public class JsonFieldAccess extends JsonSingleDataBenchmarkProgram {
	BufferedJsonRecord rec;
	public static final JsonString ID_FIELD = new JsonString("id");
	
	public JsonFieldAccess() {
		rec = new BufferedJsonRecord();
		rec.add(new JsonString(ID_FIELD), new JsonDecimal(-1));
	}
	
	@Override
	public JsonValue nextResult(JsonValue val) {
		return ((JsonRecord)val).get(ID_FIELD);
	}
}
