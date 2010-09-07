package com.ibm.jaql.benchmark;

import com.ibm.jaql.benchmark.util.BenchmarkConfig;
import com.ibm.jaql.benchmark.util.Timer;
import com.ibm.jaql.json.type.JsonRecord;

public abstract class AbstractBenchmark {
	public final static Timer TIMER = new Timer();
	private long[] duration;
	private int iterations;
	private boolean initRun = false;
	protected JsonRecord conf;

	//TODO: Should accept BenchmarkConfig not a JsonRecord
	protected void init(JsonRecord conf) throws Exception {
		iterations = BenchmarkConfig.parse(conf).getIterations();
		duration = new long[iterations];
		initRun = true;
		this.conf = conf.getCopy(null);
	}
	
	//TODO: Change visibility to public
	protected abstract void prepareIteration() throws Exception;
	protected abstract void runIteration() throws Exception;
	protected abstract void close();
	
	public final void run() throws Exception {		
		if(!initRun) {
			throw new RuntimeException("Error init Benchmark implementation, super.init() is not called");
		}
		
		for(int i=0; i<iterations; i++) {
			prepareIteration();
			TIMER.start();
			runIteration();
			TIMER.stop();
			duration[i] = TIMER.time();
			TIMER.reset();
		}
		
		close();
	}
	
	public long[] getTimings() {
		return duration;
	}
	
	public int getIterations() {
		return iterations;
	}
}
