package com.ibm.jaql.benchmark;

import com.ibm.jaql.benchmark.io.JavaInputDriver;
import com.ibm.jaql.benchmark.io.NullObjectOutput;
import com.ibm.jaql.benchmark.util.BenchmarkConfig;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;

public class JavaBenchmark extends AbstractBenchmark {
	private JavaInputDriver[] inDrivers;
	private Class<JavaBenchmarkProgram> cls;
	private String className;
	private JavaBenchmarkProgram bench;

	public JavaBenchmark(String className) {
		this.className = className;
	}

	@Override
	protected void close() {
	}

	@SuppressWarnings("unchecked")
	protected void init(JsonRecord conf) throws Exception {
		super.init(conf);
		
		cls = (Class<JavaBenchmarkProgram>) ClassLoader.getSystemClassLoader().loadClass(className);
		bench = cls.newInstance();
		
		JsonString[] inputDataFieldNames = bench.getInputDataFieldNames();
		inDrivers = new JavaInputDriver[inputDataFieldNames.length];
		
		for (int i = 0; i < inputDataFieldNames.length; i++) {
			inDrivers[i] = JavaBenchmark.getJavaInput(inputDataFieldNames[i], conf);
		}
	}
	
	@Override
	protected void prepareIteration() throws Exception {
		for (int i = 0; i < inDrivers.length; i++) {
			inDrivers[i].reset();
		}
		bench = cls.newInstance();
		bench.setInput(inDrivers);
		bench.setWriter(new NullObjectOutput());
	}

	@Override
	protected void runIteration() throws Exception {
		bench.run();
	}
	
	private static JavaInputDriver getJavaInput(JsonString dataField, JsonRecord conf) {
		BenchmarkConfig c = BenchmarkConfig.parse(conf);
		JsonValue[] values;
		try {
			values = c.getData(dataField);
		} catch (Exception e) {
			throw new RuntimeException("Could not read values", e);
		}
		
		JsonConverter converter = c.getDataConverter(dataField);
		long numberOfRecords = c.getNumberOfRecords(dataField);
		
		/* Convert json values to java objects using native types */
		Object[] objects = new Object[values.length];
		for (int i = 0; i < values.length; i++) {
			objects[i] = converter.convert(values[i]);
		}
		
		return new JavaInputDriver(objects, numberOfRecords);
	}
}
