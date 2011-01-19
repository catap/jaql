package com.ibm.jaql.benchmark.programs;

import com.ibm.jaql.benchmark.JsonBenchmarkProgramSingleInput;
import com.ibm.jaql.json.type.JsonArray;
import com.ibm.jaql.json.type.JsonLong;
import com.ibm.jaql.json.type.JsonNumber;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.type.MutableJsonLong;

public class JsonArrayAverage extends JsonBenchmarkProgramSingleInput {
	MutableJsonLong result = new MutableJsonLong();
	final JsonLong EMPTY = new JsonLong(0);

	@Override
	public JsonValue nextResult(JsonValue val) {
		JsonArray values = (JsonArray)val;
		JsonValue[] numbers = new JsonValue[(int) values.count()];
		try {
			values.getAll(numbers);
		} catch (Exception e) {
			throw new RuntimeException("Impossible");
		}

		long avg = 0;
		for (int i = 0; i < numbers.length; i++) {
			avg += ((JsonNumber)numbers[i]).longValue();
		}
		if(numbers.length>0) {
			result.set(avg/numbers.length);
			return result;
		} else {
			return EMPTY;
		}
	}

}
