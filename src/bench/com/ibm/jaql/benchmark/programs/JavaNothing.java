package com.ibm.jaql.benchmark.programs;

import com.ibm.jaql.benchmark.JavaBenchmarkProgram;

public class JavaNothing extends JavaBenchmarkProgram {
	
	@Override
	public Object nextResult(Object val) {
		return val;
	}
}
