package com.ibm.jaql.benchmark;

import com.ibm.jaql.benchmark.io.WrapperInputAdapter;
import com.ibm.jaql.io.ClosableJsonIterator;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;

public abstract class JsonBenchmarkProgramSingleInput extends JsonBenchmarkProgram {
	
	@Override
	public final void run() {
		ClosableJsonIterator in = inIterators[0];
		try {
			while(in.moveNext()) {
					writer.write(nextResult(in.current()));
			}
		} catch (Exception e) {
			throw new RuntimeException("Error during benchmark", e);
		}
	}
	
	@Override
	public JsonString[] getInputDataFieldNames() {
		return new JsonString[] { WrapperInputAdapter.DEFAULT_FIELD };
	}

	public abstract JsonValue nextResult(JsonValue val);
}
