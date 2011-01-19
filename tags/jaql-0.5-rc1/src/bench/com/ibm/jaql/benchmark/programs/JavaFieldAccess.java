package com.ibm.jaql.benchmark.programs;

import com.ibm.jaql.benchmark.JavaBenchmarkProgramSingleInput;
import com.ibm.jaql.benchmark.programs.data.Person;

public class JavaFieldAccess extends JavaBenchmarkProgramSingleInput {

	public JavaFieldAccess() {
	}

	@Override
	public Object nextResult(Object val) {
		//Notice: Java implicitly casts the int to Integer here
		return ((Person)val).getId(); 
	}
}
