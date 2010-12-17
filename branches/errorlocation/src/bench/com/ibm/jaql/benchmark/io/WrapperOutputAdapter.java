package com.ibm.jaql.benchmark.io;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.fs.Path;

import com.ibm.jaql.benchmark.AbstractBenchmark;
import com.ibm.jaql.benchmark.BenchmarkFactory;
import com.ibm.jaql.benchmark.util.BenchmarkConfig;
import com.ibm.jaql.io.AdapterStore;
import com.ibm.jaql.io.ClosableJsonWriter;
import com.ibm.jaql.io.OutputAdapter;
import com.ibm.jaql.io.AdapterStore.AdapterRegistry;
import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.type.BufferedJsonRecord;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonSchema;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.util.JaqlUtil;

public class WrapperOutputAdapter implements OutputAdapter {
	private OutputAdapter adapter = null;
	private Path location = null;
	
	@Override
	public void init(JsonValue ignored) throws Exception {
		/*
		 * Pausing the timer is required for the Jaql benchmark as there the
		 * init function is executed inside the timed block.
		 */
		AbstractBenchmark.TIMER.pause();
		/* Get settings */
		AdapterRegistry reg = AdapterStore.getStore().get(new JsonString("test"));
		JsonRecord input = reg.getOutput();
		JsonRecord conf = (JsonRecord) input.get(BenchmarkFactory.BENCH_CONF);
		JsonString serializerString = (JsonString) input.get(BenchmarkFactory.SERIALIZER);
		JsonString filesystemString = (JsonString) input.get(BenchmarkFactory.FILESYSTEM);
		
		/* Initialize the correct adapter based on the serialzer setting */
		String serializer = serializerString.toString();
		String filesystem = filesystemString.toString();
		JsonRecord adapterArgs;
		if(serializer.equalsIgnoreCase(WrapperInputAdapter.SER_NONE)) {
			adapter = new NullJsonOutput();
			adapter.init(conf);
		}
		else if(serializer.equalsIgnoreCase(WrapperInputAdapter.SER_BINARY)) {
			location = outputLocation(filesystem);			
			adapterArgs = createJaqlAdapterArgs("hdfs", location.toString());
			adapter = (OutputAdapter) JaqlUtil.getAdapterStore().output.getAdapter(adapterArgs);
		}
		else if(serializer.equalsIgnoreCase(WrapperInputAdapter.SER_TEMP)) {
			location = outputLocation(filesystem);
			adapterArgs = createJaqlAdapterArgs("jaqltemp", location.toString());
			//The jaqltemp format requires the schema to be given
			Schema s = BenchmarkConfig.parse(conf).getResultSchema();
			BufferedJsonRecord opt = new BufferedJsonRecord(1);
			opt.add(new JsonString("schema"), new JsonSchema(s));
			((BufferedJsonRecord)adapterArgs).add(OPTIONS_NAME, opt);
			adapter = (OutputAdapter) JaqlUtil.getAdapterStore().output.getAdapter(adapterArgs);
		}
		else if(serializer.equalsIgnoreCase(WrapperInputAdapter.SER_PERF)) {
			location = outputLocation(filesystem);
			adapterArgs = createJaqlAdapterArgs("perf", location.toString());
			//The jaqltemp format requires the schema to be given
			Schema s = BenchmarkConfig.parse(conf).getResultSchema();
			BufferedJsonRecord opt = new BufferedJsonRecord(1);
			opt.add(new JsonString("schema"), new JsonSchema(s));
			((BufferedJsonRecord)adapterArgs).add(OPTIONS_NAME, opt);
			adapter = (OutputAdapter) JaqlUtil.getAdapterStore().output.getAdapter(adapterArgs);
		}
		else {
			throw new RuntimeException("Invalid serializer");
		}
		AbstractBenchmark.TIMER.resume();
	}
	
	@Override
	public ClosableJsonWriter getWriter() throws Exception {
		return adapter.getWriter();
	}

	@Override
	public void close() throws Exception {
		adapter.close();
	}

	@Override
	public void open() throws Exception {
		adapter.open();
	}
	
	private Path outputLocation(String filesystem) {
		if("memory".equals(filesystem)) {
			return new Path("memory://" +  "test/null-write/null-"+System.nanoTime()+"/");
			//return new Path("memory://" +  "test/out-"+System.nanoTime());
		}
		else if("local".equals(filesystem)) {
			try {
				return new Path((new File(".")).getCanonicalPath()+ "/temp/write/out-" + System.nanoTime());
			} catch (IOException e) {
				throw new RuntimeException("lol");
			}
		}
		throw new RuntimeException("Invalid option");
	}
	
	/*
	 * Creates a record containing the basic arguments expected by the input handlers
	 */
	private JsonRecord createJaqlAdapterArgs(String name, String location) {
		BufferedJsonRecord rec = new BufferedJsonRecord(2);
		rec.add(new JsonString("type"), new JsonString(name));
		rec.add(new JsonString("location"), new JsonString(location));
		return rec;
	}

	public Path getLocation() {
		return location;
	}
	
	@Override
	public JsonValue expand() throws Exception{
		return adapter.expand();
	}
}

