package com.ibm.jaql.benchmark;

import org.apache.hadoop.conf.Configuration;

import com.ibm.jaql.benchmark.fs.MemoryFileSystem;
import com.ibm.jaql.benchmark.io.WrapperInputAdapter;
import com.ibm.jaql.io.ClosableJsonIterator;
import com.ibm.jaql.json.type.JsonRecord;

public class HadoopSerializerReadBenchmark extends AbstractBenchmark {
	MemoryFileSystem fs;
	WrapperInputAdapter inAdapter;
	ClosableJsonIterator iter;
	String type;
	long bytesRead;
	
	public HadoopSerializerReadBenchmark() {
	}
	
	@Override
	protected void init(JsonRecord conf) throws Exception {
		super.init(conf);
	    
		inAdapter = new WrapperInputAdapter();
		inAdapter.init(null); //Init values are taken from the adapter registry;
	}

	@Override
	protected void prepareIteration() throws Exception {
		inAdapter.open();
		iter = inAdapter.iter();
	}

	@Override
	protected void runIteration() throws Exception {
		try {
			while(iter.moveNext()) {
			}
			iter.close();
			inAdapter.close();
			bytesRead = inAdapter.getLocation().getFileSystem(new Configuration(true)).getFileStatus(inAdapter.getLocation()).getLen();
		} catch (Exception e) {
			throw new RuntimeException("Error during benchmark", e);
		}
	}

	@Override
	protected void close() {
	}
	
	protected long getBytesRead() {
		return bytesRead;
	}
}

