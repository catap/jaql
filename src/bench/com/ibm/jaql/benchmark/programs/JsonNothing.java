package com.ibm.jaql.benchmark.programs;

import com.ibm.jaql.benchmark.JsonBenchmarkProgramSingleInput;
import com.ibm.jaql.json.type.JsonValue;

public class JsonNothing extends JsonBenchmarkProgramSingleInput {

	@Override
	public JsonValue nextResult(JsonValue val) {
		return val;
	}
}
