package com.ibm.jaql.benchmark;

import com.ibm.jaql.benchmark.io.JavaInputDriver;
import com.ibm.jaql.benchmark.io.ObjectWriter;
import com.ibm.jaql.json.type.JsonString;

/**
 * TODO: Merge with JavaBenchmark and delete this class
 * @author kaufmannm
 *
 */
public abstract class JavaBenchmarkProgram {
	protected JavaInputDriver[] inDrivers;
	protected ObjectWriter writer;
	
	public void setInput(JavaInputDriver[] in) {
		this.inDrivers = in;
	}
	
	public void setWriter(ObjectWriter writer) {
		this.writer = writer;
	}
	
	public abstract JsonString[] getInputDataFieldNames();
	
	public abstract void run();
	
	//private final void runNormal() {
	/*public final void run() {
		Object val;
		while((val=in.getNext())!=null) {
			writer.write(nextResult(val));
		}
	}
	
	public abstract Object nextResult(Object val);
	*/
	
	/*
	private final void runAgg() throws Exception {
		Object val;
		while((val=in.getNext())!=null) {
			((JavaAggregate)this).accumulate(val);
		}
		writer.write(((JavaAggregate)this).getFinal());
	}
	*/
	
	/*
	public final void run() {
		if(this instanceof JavaAggregate) {
			try {
				runAgg();
			} catch (Exception e) {
				throw new RuntimeException("Error during benchmark", e);
			}
		} else {
			runNormal();
		}
	}
	*/
}
