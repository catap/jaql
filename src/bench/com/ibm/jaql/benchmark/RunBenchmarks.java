package com.ibm.jaql.benchmark;

import com.ibm.jaql.benchmark.util.BenchmarkConfig;
import com.ibm.jaql.benchmark.util.BenchmarkShellArguments;
import com.ibm.jaql.io.AdapterStore;
import com.ibm.jaql.io.AdapterStore.AdapterRegistry;
import com.ibm.jaql.json.type.BufferedJsonRecord;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;

public class RunBenchmarks {
	static AbstractBenchmarkFactory jaqlFactory = new JaqlBenchmarkFactory();
	static AbstractBenchmarkFactory jsonFactory = new JsonBenchmarkFactory();
	static AbstractBenchmarkFactory javaFactory = new JavaBenchmarkFactory();
	static AbstractBenchmarkFactory fsFactory = new HadoopSerializerBenchmarkFactory();
	static AbstractBenchmarkFactory rawFactory = new RawSerializerBenchmarkFactory();
	
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		String mode;
		String benchmark;
		String serializer;
		String filesystem;
		
		BenchmarkShellArguments.parseArgs(args);
		
		//Get basic options from command line
		mode = BenchmarkShellArguments.type;
		benchmark = BenchmarkShellArguments.benchmark;
		filesystem = BenchmarkShellArguments.filesystem;
		serializer = BenchmarkShellArguments.serializer;
		if(serializer == null) {
			serializer = "none";
		}
		if(filesystem == null) {
			filesystem = "memory";
		}
		
		BenchmarkConfig config = BenchmarkConfig.parse(BenchmarkConfig.getBenchmarkRecord(benchmark));
		//Add test driver to adapter store
		addTestDriver(config.getRecord(), 
				new JsonString(serializer), new JsonString(filesystem));
		
		AbstractBenchmark bench = null;
		if(mode.equalsIgnoreCase("jaql")) {
			bench = jaqlFactory.getBenchmark(benchmark);
		} else if (mode.equalsIgnoreCase("json")) {
			bench = jsonFactory.getBenchmark(benchmark);
		} else if (mode.equalsIgnoreCase("java")) {
			bench = javaFactory.getBenchmark(benchmark);
		} else if (mode.equalsIgnoreCase("hadoop-read")) {
			bench = fsFactory.getBenchmark(benchmark);
		} else if (mode.equalsIgnoreCase("hadoop-write")) {
			bench = fsFactory.getBenchmark(benchmark);
		} else if (mode.equalsIgnoreCase("raw-write")) {
			bench = rawFactory.getBenchmark(benchmark);
		} else if (mode.equalsIgnoreCase("raw-read")) {
			bench = rawFactory.getBenchmark(benchmark);
		}
		
		//TODO: When serializer is set but benchmark does not support it throw warning
		
		bench.init(config.getRecord());		
		bench.run();
		
		bench.close();
		
		long avg = 0;
		for (int i = 1; i < bench.getTimings().length; i++) {
			avg += bench.getTimings()[i];
		}
		
		//TODO: Use better methode than simple average
		if(config.getIterations() > 1) {
			avg = avg / bench.getTimings().length-1;
		}

		if(args.length < 3) {
			System.out.println("------------------------------");
			System.out.println("Average: " + avg/1000000 + "ms" + "   - Settings: " + mode + " " + benchmark);
			
			//Calculate time per records
			long timeperRecord = avg / config.getNumberOfRecords();
			System.out.println("Time per record: " + timeperRecord + " ns");
		} else {
			System.out.print(benchmark+",");
			if(BenchmarkShellArguments.modePostfix == null) {
				System.out.print(mode + ",");
			} else {
				System.out.print(mode + " " + BenchmarkShellArguments.modePostfix + ",");
			}
			System.out.print(config.getNumberOfRecords());
			for (int i = 0; i < bench.getTimings().length; i++) {
				System.out.print(",");
				System.out.print(bench.getTimings()[i]);
			}
			if (mode.equalsIgnoreCase("hadoop-read") 
					|| mode.equalsIgnoreCase("hadoop-write")
					|| mode.equalsIgnoreCase("raw-write")
					|| mode.equalsIgnoreCase("raw-read")) {
				if(bench instanceof HadoopSerializerWriteBenchmark) {
					System.out.print("," + ((HadoopSerializerWriteBenchmark)bench).getBytesWritten());
				}
				if(bench instanceof HadoopSerializerReadBenchmark) {
					System.out.print("," + ((HadoopSerializerReadBenchmark)bench).getBytesRead());
				}
				if(bench instanceof RawSerializerWriteBenchmark) {
					System.out.print("," + ((RawSerializerWriteBenchmark)bench).getBytesWritten());
				}
				if(bench instanceof RawSerializerReadBenchmark) {
					System.out.print("," + ((RawSerializerReadBenchmark)bench).getBytesRead());
				}
			}
			System.out.println();
		}
	}
	private static void addTestDriver(JsonRecord conf, JsonString serializer, JsonString filesystem) {
		BufferedJsonRecord input = new BufferedJsonRecord();
		input.set(new JsonString("adapter"), new JsonString("com.ibm.jaql.benchmark.io.WrapperInputAdapter"));
		input.set(AbstractBenchmarkFactory.BENCH_CONF, conf);
		input.set(AbstractBenchmarkFactory.SERIALIZER, serializer);
		input.set(AbstractBenchmarkFactory.FILESYSTEM, filesystem);
		BufferedJsonRecord output = new BufferedJsonRecord();
		output.set(new JsonString("adapter"), new JsonString("com.ibm.jaql.benchmark.io.WrapperOutputAdapter"));
		output.set(AbstractBenchmarkFactory.BENCH_CONF, conf);
		output.set(AbstractBenchmarkFactory.SERIALIZER, serializer);
		output.set(AbstractBenchmarkFactory.FILESYSTEM, filesystem);
		AdapterRegistry reg = new AdapterRegistry(input, output);
		AdapterStore.getStore().register(new JsonString("test"), reg);
		
		/* Add tuned serializer */
		input = new BufferedJsonRecord();
		input.set(new JsonString("adapter"), new JsonString("com.ibm.jaql.io.hadoop.PerfHadoopInputAdapter"));
		input.set(new JsonString("format"), new JsonString("org.apache.hadoop.mapred.SequenceFileInputFormat"));
		input.set(new JsonString("configurator"), new JsonString("com.ibm.jaql.io.hadoop.PerfFileInputConfigurator"));
		output = new BufferedJsonRecord();
		output.set(new JsonString("adapter"), new JsonString("com.ibm.jaql.io.hadoop.PerfHadoopOutputAdapter"));
		output.set(new JsonString("format"), new JsonString("org.apache.hadoop.mapred.SequenceFileOutputFormat"));
		output.set(new JsonString("configurator"), new JsonString("com.ibm.jaql.io.hadoop.PerfFileOutputConfigurator"));
		reg = new AdapterRegistry(input, output);
		AdapterStore.getStore().register(new JsonString("perf"), reg);
	}
}
