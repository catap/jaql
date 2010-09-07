package com.ibm.jaql.benchmark;

import com.ibm.jaql.benchmark.io.JavaInputDriver;
import com.ibm.jaql.benchmark.io.WrapperInputAdapter;
import com.ibm.jaql.json.type.JsonString;

public abstract class JavaBenchmarkProgramSingleInput extends JavaBenchmarkProgram {

	@Override
	public final void run() {
		JavaInputDriver in = inDrivers[0];
		Object val;
		while((val=in.getNext())!=null) {
			writer.write(nextResult(val));
		}
	}

	@Override
	public JsonString[] getInputDataFieldNames() {
		return new JsonString[] { WrapperInputAdapter.DEFAULT_FIELD };
	}

	public abstract Object nextResult(Object val);
}
