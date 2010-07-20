package com.ibm.jaql.benchmark;

import com.ibm.jaql.benchmark.util.BenchmarkConfig;
import com.ibm.jaql.benchmark.util.Timer;
import com.ibm.jaql.json.type.JsonRecord;

public abstract class AbstractBenchmark {
	private static Timer TIMER;
	private long[] duration;
	private int iterations;
	private boolean initRun = false;
	protected JsonRecord conf;

	protected void init(JsonRecord conf) throws Exception {
		TIMER = new Timer();
		iterations = BenchmarkConfig.parse(conf).getIterations();
		duration = new long[iterations];
		initRun = true;
		this.conf = conf.getCopy(null);
	}
	protected abstract void prepareIteration() throws Exception;
	protected abstract void runIteration() throws Exception;
	protected abstract void close();
	
	public void run() throws Exception {		
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
