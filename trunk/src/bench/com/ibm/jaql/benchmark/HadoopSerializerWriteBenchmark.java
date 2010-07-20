package com.ibm.jaql.benchmark;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import com.ibm.jaql.benchmark.fs.FsUtil;
import com.ibm.jaql.benchmark.fs.MemoryFileSystem;
import com.ibm.jaql.benchmark.io.JsonInputAdapter;
import com.ibm.jaql.benchmark.io.WrapperInputAdapter;
import com.ibm.jaql.benchmark.io.WrapperOutputAdapter;
import com.ibm.jaql.io.ClosableJsonIterator;
import com.ibm.jaql.io.ClosableJsonWriter;
import com.ibm.jaql.json.type.JsonRecord;

public class HadoopSerializerWriteBenchmark extends AbstractBenchmark {
	Path location;
	MemoryFileSystem fs;
	JsonInputAdapter inAdapter;
	WrapperOutputAdapter outAdapter;
	ClosableJsonIterator iter;
	ClosableJsonWriter writer;
	String type = "hdfs";
	long bytesWritten;
	
	public HadoopSerializerWriteBenchmark(String location) {
		this.location = new Path(location);
	}

	@Override
	protected void close() {
		try {
			outAdapter.close();
			inAdapter.close();
		} catch (Exception e) {
			throw new RuntimeException("Error closing adapters");
		}
	}

	@Override
	protected void init(JsonRecord conf) throws Exception {
		super.init(conf);
		FsUtil.setType(location, type); //TODO: Why, only necessary for reads
		
		/* Open Input Adapter */
		inAdapter = new JsonInputAdapter();
		//TODO: Use FsUtil.prepareRead
		inAdapter.init(WrapperInputAdapter.DEFAULT_FIELD, conf);
		inAdapter.setCopyRecords(false);
		
		/* Create Output Adapter */
		outAdapter = new WrapperOutputAdapter();
		outAdapter.init(null);
	}

	@Override
	protected void prepareIteration() throws Exception {
		inAdapter.open();
		outAdapter.open();
		iter = inAdapter.iter();
		writer = outAdapter.getWriter();
	}

	@Override
	protected void runIteration() throws Exception {
		try {
			while(iter.moveNext()) {
					writer.write(iter.current());
			}
			iter.close();
			writer.close();
			outAdapter.close();
			inAdapter.close();
			
			bytesWritten = outAdapter.getLocation().getFileSystem(new Configuration(true)).getFileStatus(outAdapter.getLocation()).getLen();
		} catch (Exception e) {
			throw new RuntimeException("Error during benchmark", e);
		}
	}
	
	public long getBytesWritten() {
		return bytesWritten;
	}
}
