package com.ibm.jaql.benchmark.programs;

import com.ibm.jaql.benchmark.JavaBenchmarkProgram;
import com.ibm.jaql.benchmark.programs.data.Person;

public class JavaFieldAccess extends JavaBenchmarkProgram {

	public JavaFieldAccess() {
	}

	@Override
	public Object nextResult(Object val) {
		//Notice: Java implicitly casts the int to Integer here
		return ((Person)val).getId(); 
	}
}
