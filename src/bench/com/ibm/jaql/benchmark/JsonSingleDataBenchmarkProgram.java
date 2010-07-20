package com.ibm.jaql.benchmark;

import com.ibm.jaql.io.ClosableJsonIterator;
import com.ibm.jaql.json.type.JsonValue;

public abstract class JsonSingleDataBenchmarkProgram extends JsonBenchmarkProgram {

	@Override
	public void run() {
		ClosableJsonIterator in = inIterators[0];
		try {
			while(in.moveNext()) {
					writer.write(nextResult(in.current()));
			}
		} catch (Exception e) {
			throw new RuntimeException("Error during benchmark", e);
		}
	}

	public abstract JsonValue nextResult(JsonValue val);

}
