package com.ibm.jaql.benchmark;

import com.ibm.jaql.benchmark.util.BenchmarkShellArguments;

public class RawSerializerBenchmarkFactory extends AbstractBenchmarkFactory {

	@Override
	public AbstractBenchmark getBenchmark(String name) throws Exception {
		String mode = BenchmarkShellArguments.type;
		if(mode.equals("raw-write")) {
			RawSerializerWriteBenchmark bench =  new RawSerializerWriteBenchmark();
			return bench;
		} 
		else if(mode.equals("raw-read")) {
			RawSerializerReadBenchmark bench =  new RawSerializerReadBenchmark();
			return bench;
		}
		return null;
	}

}
