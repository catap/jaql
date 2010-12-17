package com.ibm.jaql.benchmark;

import org.apache.hadoop.fs.FSDataInputStream;
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
import com.ibm.jaql.json.type.JsonLong;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;

public class RawSerializerReadBenchmark extends AbstractBenchmark {
	private Path location;
	private FSDataInputStream in;
	private BinaryFullSerializer serializer;
	private org.apache.hadoop.conf.Configuration defaultFsConf;
	private long bytesRead;
	private long numberOfRecords;
	
	public RawSerializerReadBenchmark() {
	}
	
	@Override
	protected void init(JsonRecord conf) throws Exception {
		super.init(conf);
		
		//TODO: Is bullshit use FsUtils.prepareRead
		/* Create input file */
		location = prepareRawRead(conf);
		/*
		RawSerializerWriteBenchmark bench = new RawSerializerWriteBenchmark();
	    bench.init(conf);
	    bench.prepareIteration();
	    bench.runIteration();
	    location = bench.location;
	    */
	    
		/* Get settings */
	    //TODO: Use same field that is also used for creation, this is currently hidden by using
	    //the RawSerializerWriteBenchark for writing.
	    numberOfRecords = BenchmarkConfig.parse(conf).getNumberOfRecords(WrapperInputAdapter.DEFAULT_FIELD);
		defaultFsConf = new org.apache.hadoop.conf.Configuration(true);
		AdapterRegistry reg = AdapterStore.getStore().get(new JsonString("test"));
		JsonRecord input = reg.getOutput();
		JsonString serializerString = (JsonString) input.get(BenchmarkFactory.SERIALIZER);
		
		/* Create Deserializer*/
		serializer = RawSerializerUtil.getSerializer(serializerString.toString(),
													 BenchmarkConfig.parse(conf).getDataSchema(WrapperInputAdapter.DEFAULT_FIELD));
	}

	@Override
	protected void prepareIteration() throws Exception {
		in = location.getFileSystem(defaultFsConf).open(location);
		bytesRead = location.getFileSystem(defaultFsConf).getFileStatus(location).getLen();
	}

	@Override
	protected void runIteration() throws Exception {
		try {
			for (int i = 0; i < numberOfRecords; i++) {
				//serializer.read(in, null);
				serializer.read(in, null);
			}
			in.close();
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
	
	private Path prepareRawRead(JsonRecord conf) {
		JsonInputAdapter inAdapter;
		ClosableJsonIterator iter;
		FSDataOutputStream out;
				
		try {
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
			location = RawSerializerUtil.outputNonNullLocation(filesystemString.toString());
			
			/* Create Output Stream*/
			serializer = RawSerializerUtil.getSerializer(serializerString.toString(),
					BenchmarkConfig.parse(conf).getDataSchema(WrapperInputAdapter.DEFAULT_FIELD));
			
			inAdapter.open();
			iter = inAdapter.iter();
			out = location.getFileSystem(defaultFsConf).create(location, true);
			
			while(iter.moveNext()) {
				serializer.write(out, iter.current());
			}
			
			iter.close();
			inAdapter.close();
			out.close();
			
			return location;
		} catch (Exception e) {
			throw new RuntimeException("Error during benchmark", e);
		}
	}
}

