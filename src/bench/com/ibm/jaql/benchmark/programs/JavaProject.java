package com.ibm.jaql.benchmark.programs;

import com.ibm.jaql.benchmark.JavaBenchmarkProgram;
import com.ibm.jaql.benchmark.programs.data.Person;
import com.ibm.jaql.benchmark.programs.data.Name;

public class JavaProject extends JavaBenchmarkProgram {
	
	private Name p;

	public JavaProject() {
		p = new Name();
	}

	@Override
	public Object nextResult(Object val) {
		Person person = (Person) val;
		p.setId(person.getId());
		p.setName(person.getLastname());
		
		return p;
	}
}
