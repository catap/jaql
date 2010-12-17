package com.ibm.jaql.benchmark.programs;

import com.ibm.jaql.benchmark.JavaBenchmarkProgramSingleInput;
import com.ibm.jaql.benchmark.programs.data.Person;
import com.ibm.jaql.benchmark.programs.data.JoinPersonReduce.F;

public class JavaJoinPersonMap extends JavaBenchmarkProgramSingleInput {
	Object[] result = new Object[2];
	@Override
	public Object nextResult(Object val) {
		Person p = (Person) val;
		
		F wrapped = new F();
		wrapped.setPerson(p);
		
		result[0] = p.getLastname();
		result[1] = wrapped;
		return result;
	}

}
