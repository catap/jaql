package com.ibm.jaql.benchmark;

import com.ibm.jaql.io.ClosableJsonIterator;
import com.ibm.jaql.io.ClosableJsonWriter;
import com.ibm.jaql.json.type.JsonString;

/**
 * TODO: Merge with JsonBenchmark and delete this class
 * @author kaufmannm
 *
 */
public abstract class JsonBenchmarkProgram {
	protected ClosableJsonIterator[] inIterators;
	protected ClosableJsonWriter writer;
	
	public void setInput(ClosableJsonIterator[] inIterators) {
		this.inIterators = inIterators;
	}
	
	public void setWriter(ClosableJsonWriter writer) {
		this.writer = writer;
	}

	/*
	 * The input adapters are initialized with the data from the fields
	 * in the config with the names returned by this function.
	 */
	public abstract JsonString[] getInputDataFieldNames();

	public abstract void run();
}
