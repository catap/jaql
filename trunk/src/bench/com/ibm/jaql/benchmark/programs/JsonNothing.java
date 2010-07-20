package com.ibm.jaql.benchmark.programs;

import com.ibm.jaql.benchmark.JsonSingleDataBenchmarkProgram;
import com.ibm.jaql.json.type.JsonValue;

public class JsonNothing extends JsonSingleDataBenchmarkProgram {

	@Override
	public JsonValue nextResult(JsonValue val) {
		return val;
	}
}
