package com.ibm.jaql.benchmark;

import com.ibm.jaql.benchmark.util.BenchmarkShellArguments;

public class HadoopSerializerBenchmarkFactory extends AbstractBenchmarkFactory {

	@Override
	public AbstractBenchmark getBenchmark(String name) throws Exception {
		String mode = BenchmarkShellArguments.type;
		if(mode.equalsIgnoreCase("hadoop-write")) {
			HadoopSerializerWriteBenchmark bench =  new HadoopSerializerWriteBenchmark("memory://test/null-tstout");
			return bench;
		} else if(mode.equalsIgnoreCase("hadoop-read")) {
			HadoopSerializerReadBenchmark bench =  new HadoopSerializerReadBenchmark();
			return bench;
		}
		return null;
	}

}
