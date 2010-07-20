package com.ibm.jaql.benchmark;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;

import com.ibm.jaql.benchmark.util.BenchmarkConfig;
import com.ibm.jaql.benchmark.util.RawSerializerUtil;
import com.ibm.jaql.io.AdapterStore;
import com.ibm.jaql.io.AdapterStore.AdapterRegistry;
import com.ibm.jaql.io.serialization.binary.BinaryFullSerializer;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;

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
		
		/* Create input file */
		RawSerializerWriteBenchmark bench = new RawSerializerWriteBenchmark();
	    bench.init(conf);
	    bench.prepareIteration();
	    bench.runIteration();
	    location = bench.location;
	    
		/* Get settings */
	    numberOfRecords = BenchmarkConfig.parse(conf).getNumberOfRecords();
		defaultFsConf = new org.apache.hadoop.conf.Configuration(true);
		AdapterRegistry reg = AdapterStore.getStore().get(new JsonString("test"));
		JsonRecord input = reg.getOutput();
		JsonString serializerString = (JsonString) input.get(AbstractBenchmarkFactory.SERIALIZER);
		
		/* Create Deserializer*/
		serializer = RawSerializerUtil.getSerializer(serializerString.toString(),
				BenchmarkConfig.parse(conf).getResultSchema());
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
}

