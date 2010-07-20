package com.ibm.jaql.benchmark.programs;

import com.ibm.jaql.benchmark.JavaBenchmarkProgram;

public class JavaStringConcat extends JavaBenchmarkProgram {

	@Override
	public Object nextResult(Object val) {
		String[] values = (String[])val;
		return values[0] + " " + values[1];
	}

}
