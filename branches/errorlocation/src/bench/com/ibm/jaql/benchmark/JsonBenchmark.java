package com.ibm.jaql.benchmark;

import com.ibm.jaql.benchmark.io.WrapperInputAdapter;
import com.ibm.jaql.benchmark.io.WrapperOutputAdapter;
import com.ibm.jaql.io.ClosableJsonIterator;
import com.ibm.jaql.io.ClosableJsonWriter;
import com.ibm.jaql.io.InputAdapter;
import com.ibm.jaql.io.OutputAdapter;
import com.ibm.jaql.json.type.BufferedJsonRecord;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;

public class JsonBenchmark extends AbstractBenchmark {
	InputAdapter[] inAdapters;
	OutputAdapter outAdapter;
	ClosableJsonIterator[] inIterators;
	ClosableJsonWriter writer;
	String className;
	Class<JsonBenchmarkProgram> cls;
	JsonBenchmarkProgram bench;
	
	public JsonBenchmark(String className) {
		this.className =className;
	}
	
	@Override
	protected void close() {
	}

	@SuppressWarnings("unchecked")
	@Override
	protected void init(JsonRecord conf) throws Exception {
		super.init(conf);
		
		cls = (Class<JsonBenchmarkProgram>) ClassLoader.getSystemClassLoader().loadClass(className);
		bench = cls.newInstance();
		
		JsonString[] dataFieldNames = bench.getInputDataFieldNames();
		inAdapters = new InputAdapter[dataFieldNames.length];
		inIterators = new ClosableJsonIterator[dataFieldNames.length];
		for (int i = 0; i < dataFieldNames.length; i++) {
			inAdapters[i] = new WrapperInputAdapter();
			//Create init config record and use it for initialization
			BufferedJsonRecord adapterConf = new BufferedJsonRecord(1);
			adapterConf.add(WrapperInputAdapter.DATA, dataFieldNames[i]);
			inAdapters[i].init(adapterConf);
		}
		
		outAdapter = new WrapperOutputAdapter();
		outAdapter.init(conf);
	}
	
	@Override
	protected void prepareIteration() throws Exception {
		for (int i = 0; i < inAdapters.length; i++) {
			inAdapters[i].open();
			inIterators[i] = inAdapters[i].iter();
		}
		
		outAdapter.open();
		writer = outAdapter.getWriter();
		bench = cls.newInstance();
		bench.setInput(inIterators);
		bench.setWriter(writer);
	}

	@Override
	protected void runIteration() throws Exception {
		bench.run();
		for (int i = 0; i < inAdapters.length; i++) {
			inIterators[i].close();
			inAdapters[i].close();
			writer.close();
			outAdapter.close();
		}
	}
}
