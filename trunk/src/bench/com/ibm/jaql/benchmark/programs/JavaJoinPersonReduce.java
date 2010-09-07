package com.ibm.jaql.benchmark.programs;

import java.util.LinkedList;

import com.ibm.jaql.benchmark.JavaBenchmarkProgramSingleInput;
import com.ibm.jaql.benchmark.programs.data.JoinPersonReduce;
import com.ibm.jaql.benchmark.programs.data.JoinPersonReduce.F;
import com.ibm.jaql.benchmark.programs.data.JoinPersonReduce.JoinResult;
import com.ibm.jaql.benchmark.programs.data.JoinPersonReduce.L;

public class JavaJoinPersonReduce extends JavaBenchmarkProgramSingleInput {
	
	@Override
	public Object nextResult(Object val) {
		JoinPersonReduce data = (JoinPersonReduce) val;
		
		F[] valuesA = data.getValuesA();
		L[] valuesB = data.getValuesB();
		
		LinkedList<JoinResult> results = new LinkedList<JoinResult>();
		
		for (int i = 0; i < valuesA.length; i++) {
			for (int j = 0; j < valuesB.length; j++) {
				JoinResult result = new JoinResult();
				result.setIdA(valuesA[i].getPerson().getId());
				result.setIdB(valuesB[j].getPerson().getId());
				results.add(result);
			}
		}
		
		return results;
	}

}
