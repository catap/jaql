package com.ibm.jaql.benchmark.io;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.fs.Path;

import com.ibm.jaql.benchmark.AbstractBenchmarkFactory;
import com.ibm.jaql.benchmark.fs.FsUtil;
import com.ibm.jaql.benchmark.util.BenchmarkConfig;
import com.ibm.jaql.io.AdapterStore;
import com.ibm.jaql.io.ClosableJsonIterator;
import com.ibm.jaql.io.InputAdapter;
import com.ibm.jaql.io.AdapterStore.AdapterRegistry;
import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.type.BufferedJsonRecord;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonSchema;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.util.JaqlUtil;

//TODO: Check that records are not changed during run
public class WrapperInputAdapter implements InputAdapter {
	protected static String SER_NONE = "none";
	protected static String SER_BINARY = "hdfs";
	protected static String SER_TEMP ="jaqltemp";
	protected static String SER_PERF ="perf";
	protected static JsonString DATA = new JsonString("data");
	public static JsonString DATA_FIELD = new JsonString("dataField");
	public static JsonString DEFAULT_FIELD = new JsonString("data");
	
	private InputAdapter adapter = null;
	private Path location = null;
	
	@Override
	public void init(JsonValue args) throws Exception {
		/* Get settings */
		AdapterRegistry reg = AdapterStore.getStore().get(new JsonString("test"));
		JsonRecord input = reg.getInput();
		JsonRecord conf = (JsonRecord) input.get(AbstractBenchmarkFactory.BENCH_CONF);
		
		/* Get data for this specific input adapter based on the args or use the
		 * default value
		 */
		JsonString dataField = null;
		if(args != null) {
			dataField = (JsonString) ((JsonRecord)args).get(WrapperInputAdapter.DATA);
		}
		if (dataField == null) {
			dataField = WrapperInputAdapter.DEFAULT_FIELD;
		}
		
		/* Check whether data exists */
		if(conf.get(dataField) == null) {
			throw new RuntimeException("Data does not exist in configuration: " + dataField);
		}
		
		JsonString serializerString = (JsonString) input.get(AbstractBenchmarkFactory.SERIALIZER);
		JsonString filesystemString = (JsonString) input.get(AbstractBenchmarkFactory.FILESYSTEM);
		
		/* Initialize the correct adapter based on the serialzer setting */
		String serializer = serializerString.toString();
		String filesystem = filesystemString.toString();
		JsonRecord adapterArgs;
		if(serializer.equalsIgnoreCase(SER_NONE)) {
			adapter = new JsonInputAdapter();
			((JsonInputAdapter)adapter).init(dataField, conf);
		}
		else if(serializer.equalsIgnoreCase(SER_BINARY)) {
			location = inputLocation(filesystem);
			FsUtil.prepareRead(location, "hdfs", filesystem, dataField, conf);
			
			adapterArgs = createJaqlAdapterArgs("hdfs", location.toString());
			adapter = (InputAdapter) JaqlUtil.getAdapterStore().input.getAdapter(adapterArgs);
		}
		else if(serializer.equalsIgnoreCase(SER_TEMP)) {
			location = inputLocation(filesystem);
			FsUtil.prepareRead(location, "jaqltemp", filesystem, dataField, conf);
			
			adapterArgs = createJaqlAdapterArgs("jaqltemp", location.toString());
			//The jaqltemp format requires the schema to be given
			Schema s = BenchmarkConfig.parse(conf).getDataSchema(dataField);
			BufferedJsonRecord opt = new BufferedJsonRecord(1);
			opt.add(new JsonString("schema"), new JsonSchema(s));
			((BufferedJsonRecord)adapterArgs).add(OPTIONS_NAME, opt);
			adapter = (InputAdapter) JaqlUtil.getAdapterStore().input.getAdapter(adapterArgs);
		}
		else if(serializer.equalsIgnoreCase(SER_PERF)) {
			location = inputLocation(filesystem);
			FsUtil.prepareRead(location, "perf", filesystem, dataField, conf);
			
			adapterArgs = createJaqlAdapterArgs("perf", location.toString());
			//The jaqltemp format requires the schema to be given
			Schema s = BenchmarkConfig.parse(conf).getDataSchema(dataField);
			BufferedJsonRecord opt = new BufferedJsonRecord(1);
			opt.add(new JsonString("schema"), new JsonSchema(s));
			((BufferedJsonRecord)adapterArgs).add(OPTIONS_NAME, opt);
			adapter = (InputAdapter) JaqlUtil.getAdapterStore().input.getAdapter(adapterArgs);
		}
		else {
			throw new RuntimeException("Invalid serializer");
		}
	}

	@Override
	public Schema getSchema() {
		return adapter.getSchema();
	}

	@Override
	public ClosableJsonIterator iter() throws Exception { 
		return adapter.iter();
	}

	@Override
	public void close() throws Exception {
		adapter.close();
	}

	@Override
	public void open() throws Exception {
		adapter.open();
	}
	
	public Path getLocation() {
		return location;
	}
	
	private Path inputLocation(String filesystem) {		
		if("memory".equals(filesystem)) {
			//return new Path("memory://" +  "/test/gen/in-"+System.nanoTime());
			//TODO: Bug in fs implementation
			return new Path("memory://" +  "test/in-"+System.nanoTime());
		}
		else if("local".equals(filesystem)) {
			try {
				return new Path((new File(".")).getCanonicalPath()+ "/temp/in-" + System.nanoTime());
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
}
