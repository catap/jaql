package com.ibm.jaql.benchmark.programs;

import com.ibm.jaql.benchmark.JavaBenchmarkProgram;

public class JavaArrayAverage extends JavaBenchmarkProgram {

	@Override
	public Object nextResult(Object val) {
		int[] decimals = (int[])val;
		
		long avg = 0;
		for (int i = 0; i < decimals.length; i++) {
			avg += decimals[i];
		}
		
		return decimals.length>0?avg / decimals.length:0;
	}

}
