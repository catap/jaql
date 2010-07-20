package com.ibm.jaql.benchmark;

import com.ibm.jaql.io.ClosableJsonIterator;
import com.ibm.jaql.io.ClosableJsonWriter;
import com.ibm.jaql.json.type.JsonRecord;

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

	public JsonRecord[] getInputArguments() {
		return new JsonRecord[1];
	}

	public abstract void run();
}
