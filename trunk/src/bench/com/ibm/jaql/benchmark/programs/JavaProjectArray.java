package com.ibm.jaql.benchmark.programs;

import com.ibm.jaql.benchmark.JavaBenchmarkProgram;

public class JavaProjectArray extends JavaBenchmarkProgram {
	Object[] arr;

	public JavaProjectArray() {
		arr = new Object[2];
	}

	@Override
	public Object nextResult(Object val) {
		Object[] person = (Object[]) val;
		arr[0] = person[0];
		arr[1] = person[2];
		
		return arr;
	}
}
