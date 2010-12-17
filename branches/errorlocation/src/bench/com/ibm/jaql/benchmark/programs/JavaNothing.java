package com.ibm.jaql.benchmark.programs;

import com.ibm.jaql.benchmark.JavaBenchmarkProgramSingleInput;

public class JavaNothing extends JavaBenchmarkProgramSingleInput {
	
	@Override
	public Object nextResult(Object val) {
		return val;
	}
}
