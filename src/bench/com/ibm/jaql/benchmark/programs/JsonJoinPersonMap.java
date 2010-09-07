package com.ibm.jaql.benchmark.programs;

import com.ibm.jaql.benchmark.JsonBenchmarkProgramSingleInput;
import com.ibm.jaql.json.type.BufferedJsonArray;
import com.ibm.jaql.json.type.BufferedJsonRecord;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.util.FieldNameCache;

public class JsonJoinPersonMap extends JsonBenchmarkProgramSingleInput {
	public static final JsonString KEY_FIELD = FieldNameCache.get(new JsonString("forename"));
	public static final JsonString F = FieldNameCache.get(new JsonString("f"));
	public static final JsonString L = FieldNameCache.get(new JsonString("l"));
	
	private BufferedJsonArray arr;
	private BufferedJsonRecord recF;
	private JsonValue[] recValues;
	private JsonValue[] arrValues;
	
	public JsonJoinPersonMap() {
		recF = new BufferedJsonRecord();
		recF.add(F, new JsonString("invalid"));
		recValues = new JsonValue[1];
		
		arr = new BufferedJsonArray();
		arrValues = new JsonValue[2];
	}
	
	@Override
	public JsonValue nextResult(JsonValue val) {
		JsonRecord person = (JsonRecord) val;
		
		recValues[0] = person;
		recF.set(recValues);
		
		arrValues[0] = person.get(KEY_FIELD);
		arrValues[1] = recF;
		
		arr.set(arrValues, 2);
		return arr;
	}

}
