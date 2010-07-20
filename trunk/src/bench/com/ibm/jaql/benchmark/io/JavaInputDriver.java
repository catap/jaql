package com.ibm.jaql.benchmark.io;

import java.util.Random;

public class JavaInputDriver {
	Object[] data;
	long numberOfRecords;
	int recordCount;
	Random rnd = new Random();
	
	public JavaInputDriver(Object[] data, long numberOfRecords) {
		this.numberOfRecords = numberOfRecords;
		this.data = data;
		
		reset();
	}
	
	public Object getNext() {
		//AbstractBenchmark.TIMER.pause();
		recordCount++;
		Object o = recordCount<=numberOfRecords?data[rnd.nextInt(data.length)]:null;
		//AbstractBenchmark.TIMER.resume();
		return o;
	}
	
	public void reset() {
		recordCount = 0;
		rnd.setSeed(1988);
	}
}
