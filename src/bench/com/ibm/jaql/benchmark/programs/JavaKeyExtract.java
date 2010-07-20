package com.ibm.jaql.benchmark.programs;

import com.ibm.jaql.benchmark.JavaBenchmarkProgram;
import com.ibm.jaql.benchmark.programs.data.KeyValue;
import com.ibm.jaql.benchmark.programs.data.Person;

public class JavaKeyExtract extends JavaBenchmarkProgram {
	KeyValue kv = new KeyValue(null, null);

	@Override
	public Object nextResult(Object val) {
		Person p = (Person) val;
		kv.setKey(p.getForename());
		kv.setValue(p);
		return kv;
	}


}
