package com.ibm.jaql.benchmark;

import com.ibm.jaql.benchmark.io.JavaInputDriver;
import com.ibm.jaql.benchmark.io.ObjectWriter;

/**
 * TODO: Merge with JavaBenchmark and delete this class
 * @author kaufmannm
 *
 */
public abstract class JavaBenchmarkProgram {
	protected JavaInputDriver in;
	private ObjectWriter writer;
	
	public void setInput(JavaInputDriver in) {
		this.in = in;
	}
	
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
	
	private final void runNormal() {
		Object val;
		while((val=in.getNext())!=null) {
			writer.write(nextResult(val));
		}
	}
	
	private final void runAgg() throws Exception {
		Object val;
		while((val=in.getNext())!=null) {
			((JavaAggregate)this).accumulate(val);
		}
		writer.write(((JavaAggregate)this).getFinal());
	}
	
	public abstract Object nextResult(Object val);
	
	public void setWriter(ObjectWriter writer) {
		this.writer = writer;
	}
}
