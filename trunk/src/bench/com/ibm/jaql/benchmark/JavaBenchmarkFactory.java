package com.ibm.jaql.benchmark;

import com.ibm.jaql.benchmark.util.BenchmarkConfig;
import com.ibm.jaql.json.type.JsonString;

public class JavaBenchmarkFactory extends AbstractBenchmarkFactory {
	public static final JsonString JAVA_BENCH = new JsonString("javaBench");
	public static final JsonString JSON_CONVERTER = new JsonString("jsonConverter");

	@Override
	public AbstractBenchmark getBenchmark(String name) throws Exception {
		String className = BenchmarkConfig.parse(BenchmarkConfig.getBenchmarkRecord(name)).getJavaBenchmark();
		return new JavaBenchmark(className);
	}
}
