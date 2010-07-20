package com.ibm.jaql.benchmark;

import com.ibm.jaql.benchmark.util.BenchmarkConfig;

public class JaqlBenchmarkFactory extends AbstractBenchmarkFactory {	
	@Override
	public AbstractBenchmark getBenchmark(String name) throws Exception {
		String scriptLocation = BenchmarkConfig.parse(BenchmarkConfig.getBenchmarkRecord(name)).getJaqlScriptLocation();
		return new JaqlBenchmark(scriptLocation);
	}
}
