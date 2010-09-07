package com.ibm.jaql.benchmark.programs;

import com.ibm.jaql.benchmark.JsonBenchmarkProgramSingleInput;
import com.ibm.jaql.json.type.BufferedJsonRecord;
import com.ibm.jaql.json.type.JsonDecimal;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
//import com.ibm.jaql.lang.util.JsonStringCache;

public class JsonProject extends JsonBenchmarkProgramSingleInput {
	BufferedJsonRecord rec;
	public static final JsonString NAME_FIELD = new JsonString("lastname"); //JsonStringCache.get(new JsonString("lastname"));
	public static final JsonString ID_FIELD = new JsonString("id"); //JsonStringCache.get(new JsonString("id"));
	
	public JsonProject() {
		rec = new BufferedJsonRecord();
		rec.add(NAME_FIELD, new JsonString("invalid"));
		rec.add(ID_FIELD, new JsonDecimal(-1));
	}
	
	@Override
	public JsonValue nextResult(JsonValue val) {
		//TODO: Set by using arrays
		JsonRecord person = (JsonRecord) val;
		rec.set(NAME_FIELD, person.get(NAME_FIELD));
		rec.set(ID_FIELD, person.get(ID_FIELD));
		
		return rec;
	}
}
