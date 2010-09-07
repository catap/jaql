package com.ibm.jaql.benchmark.programs;

import com.ibm.jaql.benchmark.JavaBenchmarkProgramSingleInput;
import com.ibm.jaql.benchmark.programs.data.Name;
import com.ibm.jaql.benchmark.programs.data.Person;

public class JavaTransform extends JavaBenchmarkProgramSingleInput {
	
	private Name p;

	public JavaTransform() {
		p = new Name();
	}

	@Override
	public Object nextResult(Object val) {
		Person person = (Person) val;
		p.setId(person.getId());
		p.setName(person.getForename() + " " + person.getLastname());
		
		return p;
	}
}
