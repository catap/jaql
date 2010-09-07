package com.ibm.jaql.benchmark;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;

import com.ibm.jaql.benchmark.io.JsonInputAdapter;
import com.ibm.jaql.benchmark.io.WrapperInputAdapter;
import com.ibm.jaql.benchmark.util.BenchmarkConfig;
import com.ibm.jaql.benchmark.util.RawSerializerUtil;
import com.ibm.jaql.io.AdapterStore;
import com.ibm.jaql.io.ClosableJsonIterator;
import com.ibm.jaql.io.AdapterStore.AdapterRegistry;
import com.ibm.jaql.io.serialization.binary.BinaryFullSerializer;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;

public class RawSerializerWriteBenchmark extends AbstractBenchmark {
	protected Path location;
	private JsonInputAdapter inAdapter;
	private ClosableJsonIterator iter;
	private FSDataOutputStream out;
	private org.apache.hadoop.conf.Configuration defaultFsConf;
	private BinaryFullSerializer serializer;
	private long bytesWritten;
	
	public RawSerializerWriteBenchmark() {
	}

	@Override
	protected void close() {
		try {
			out.close();
		} catch (IOException e) {
			throw new RuntimeException("Nooooooo");
		}
	}

	@Override
	protected void init(JsonRecord conf) throws Exception {
		super.init(conf);
		
		/* Open Input Adapter */
		inAdapter = new JsonInputAdapter();
		inAdapter.init(WrapperInputAdapter.DEFAULT_FIELD, conf);
		inAdapter.setCopyRecords(false);
		
		/* Get settings */
		defaultFsConf = new org.apache.hadoop.conf.Configuration(true);
		AdapterRegistry reg = AdapterStore.getStore().get(new JsonString("test"));
		JsonRecord input = reg.getOutput();
		JsonString serializerString = (JsonString) input.get(BenchmarkFactory.SERIALIZER);
		JsonString filesystemString = (JsonString) input.get(BenchmarkFactory.FILESYSTEM);
		location = RawSerializerUtil.outputLocation(filesystemString.toString());
		
		/* Create Output Stream*/
		serializer = RawSerializerUtil.getSerializer(serializerString.toString(),
				BenchmarkConfig.parse(conf).getDataSchema(WrapperInputAdapter.DEFAULT_FIELD));
	}

	@Override
	protected void prepareIteration() throws Exception {
		inAdapter.open();
		iter = inAdapter.iter();
		out = location.getFileSystem(defaultFsConf).create(location, true);
	}

	@Override
	protected void runIteration() throws Exception {
		try {
			while(iter.moveNext()) {
				serializer.write(out, iter.current());
			}
			
			iter.close();
			inAdapter.close();
			out.close();
			bytesWritten = location.getFileSystem(defaultFsConf).getFileStatus(location).getLen();
		} catch (Exception e) {
			throw new RuntimeException("Error during benchmark", e);
		}
	}
	
	public long getBytesWritten() {
		return bytesWritten;
	}
}
