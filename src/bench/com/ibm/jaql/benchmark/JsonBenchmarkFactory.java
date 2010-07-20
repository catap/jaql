package com.ibm.jaql.benchmark;

import com.ibm.jaql.benchmark.util.BenchmarkConfig;
import com.ibm.jaql.json.type.JsonString;

public class JsonBenchmarkFactory extends AbstractBenchmarkFactory {
	public static final JsonString JSON_BENCH = new JsonString("jsonBench");

	@Override
	public AbstractBenchmark getBenchmark(String name) throws Exception {
		String className = BenchmarkConfig.parse(BenchmarkConfig.getBenchmarkRecord(name)).getJsonBenchmark();		
		return new JsonBenchmark(className);
	}
}
