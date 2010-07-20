package com.ibm.jaql.benchmark;

import com.ibm.jaql.json.type.JsonString;

//TODO: Use registry pattern
public abstract class AbstractBenchmarkFactory {
	//TODO: Read from Shell Arguments
	public static final JsonString BENCH_CONF = new JsonString("benchConfig");
	public static final JsonString SERIALIZER = new JsonString("serializer");
	public static final JsonString FILESYSTEM = new JsonString("filesystem");
	
	
	public abstract AbstractBenchmark getBenchmark(String name)
			throws Exception;
}
