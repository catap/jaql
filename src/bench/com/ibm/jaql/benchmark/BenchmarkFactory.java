package com.ibm.jaql.benchmark;

import com.ibm.jaql.benchmark.util.BenchmarkConfig;
import com.ibm.jaql.json.type.JsonString;

/*
 * TODO: Use registry pattern and only allow default constructor for Benchmarks
 * to make it more easier extendable
 */
public final class BenchmarkFactory {
	//TODO: Read from Shell Arguments
	public static final JsonString BENCH_CONF = new JsonString("benchConfig");
	public static final JsonString SERIALIZER = new JsonString("serializer");
	public static final JsonString FILESYSTEM = new JsonString("filesystem");
	
	
	public static AbstractBenchmark getBenchmark(String mode, String benchmark) throws Exception {
		if(mode.equalsIgnoreCase("jaql")) {
			String scriptLocation = BenchmarkConfig.parse(BenchmarkConfig.getBenchmarkRecord(benchmark)).getJaqlScriptLocation();
			return new JaqlBenchmark(scriptLocation);
		} else if (mode.equalsIgnoreCase("json")) {
			String className = BenchmarkConfig.parse(BenchmarkConfig.getBenchmarkRecord(benchmark)).getJsonBenchmark();		
			return new JsonBenchmark(className);
		} else if (mode.equalsIgnoreCase("java")) {
			String className = BenchmarkConfig.parse(BenchmarkConfig.getBenchmarkRecord(benchmark)).getJavaBenchmark();
			return new JavaBenchmark(className);
		} else if (mode.equalsIgnoreCase("hadoop-read")) {
			HadoopSerializerReadBenchmark bench =  new HadoopSerializerReadBenchmark();
			return bench;
		} else if (mode.equalsIgnoreCase("hadoop-write")) {
			//HadoopSerializerWriteBenchmark bench =  new HadoopSerializerWriteBenchmark("memory://test/null-tstout");
			HadoopSerializerWriteBenchmark bench =  new HadoopSerializerWriteBenchmark();
			return bench;
		} else if (mode.equalsIgnoreCase("raw-read")) {
			RawSerializerReadBenchmark bench =  new RawSerializerReadBenchmark();
			return bench;
		} else if (mode.equalsIgnoreCase("raw-write")) {
			RawSerializerWriteBenchmark bench =  new RawSerializerWriteBenchmark();
			return bench;
		} else {
			throw new RuntimeException("Error: Invalid benchmark option");
		}		
	}
}
