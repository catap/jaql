package com.ibm.jaql.benchmark;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.FileReader;
import java.io.InputStream;
import java.io.InputStreamReader;

import com.ibm.jaql.benchmark.io.WrapperOutputAdapter;
import com.ibm.jaql.benchmark.lang.JaqlPrecompile;
import com.ibm.jaql.io.ClosableJsonWriter;
import com.ibm.jaql.io.OutputAdapter;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.lang.IODescriptorPrinter;

public class JaqlBenchmark extends AbstractBenchmark {
	ClosableJsonWriter outWriter;
	private byte[] sourceBytes;
	private String scriptLocation;
	private JaqlPrecompile engine;
	private OutputAdapter outAdapter;
	
	public JaqlBenchmark(String location) {
		scriptLocation = location;
	}
	
	@Override
	public void close() {
	}

	@Override
	public void init(JsonRecord conf) throws Exception {
		super.init(conf);
		
		// Read source file
		StringBuffer source = new StringBuffer();
		String scriptPath = ClassLoader.getSystemResource(scriptLocation).getPath();
		BufferedReader in = new BufferedReader(new FileReader(scriptPath));
		String str;
		while ((str = in.readLine()) != null) {
			source.append(str+System.getProperty("line.separator"));
		}
		in.close();

		sourceBytes = source.toString().getBytes("UTF-8");
		
		outAdapter = new WrapperOutputAdapter();
		outAdapter.init(conf);
	}
	

	@Override
	protected void prepareIteration() throws Exception {
		InputStream in = new ByteArrayInputStream(sourceBytes);
		outAdapter.open();
		outWriter = outAdapter.getWriter();
		
		engine = new JaqlPrecompile("<stdin>", new InputStreamReader(in));
		engine.setJaqlPrinter(new IODescriptorPrinter(outWriter));
		engine.precompile();
	}

	@Override
	protected void runIteration() throws Exception {
		engine.run();
		outWriter.close();
		outAdapter.close();
	}
}
