package com.ibm.jaql.benchmark.programs;

import com.ibm.jaql.benchmark.JsonSingleDataBenchmarkProgram;
import com.ibm.jaql.json.type.BufferedJsonRecord;
import com.ibm.jaql.json.type.JsonDecimal;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
//import com.ibm.jaql.lang.util.JsonStringCache;

public class JsonProjectTuned extends JsonSingleDataBenchmarkProgram {
	BufferedJsonRecord rec;
	JsonValue[] values;
	public static final JsonString NAME_FIELD = new JsonString("lastname"); //JsonStringCache.get("lastname");
	public static final JsonString ID_FIELD = new JsonString("id"); //JsonStringCache.get("id");
	
	public JsonProjectTuned() {
		rec = new BufferedJsonRecord();
		rec.add(NAME_FIELD, new JsonString("invalid"));
		rec.add(ID_FIELD, new JsonDecimal(-1));
		rec.sort();
		
		values = new JsonValue[2];
	}
	
	@Override
	public JsonValue nextResult(JsonValue val) {
		JsonRecord person = (JsonRecord) val;
		values[0] = person.get(ID_FIELD);
		values[1] = person.get(NAME_FIELD);
		rec.set(values);
		
		return rec;
	}
}
