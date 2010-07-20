package com.ibm.jaql.benchmark.fs;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.fs.Path;

import com.ibm.jaql.benchmark.util.BenchmarkConfig;
import com.ibm.jaql.io.Adapter;
import com.ibm.jaql.io.ClosableJsonWriter;
import com.ibm.jaql.io.OutputAdapter;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.type.BufferedJsonRecord;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonSchema;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.util.JaqlUtil;

public class FsUtil {
	private static long sequenceHeaderSize = -1;
	private static Map<Path, String> pathType = new HashMap<Path, String>();
	
	/* Only accepts absolute paths */
	public static void prepareRead(Path location, String type, String filesystem, JsonString dataField, JsonRecord conf) throws Exception {
		BenchmarkConfig config = BenchmarkConfig.parse(conf);
		FsUtil.setType(location, type);
		Random rnd = new Random();
		rnd.setSeed(1988);
		long numberOfRecords = config.getNumberOfRecords();
		//int repetitions = numberOfRecords / 100;
		JsonValue[] values = config.getData(dataField);
		
		BufferedJsonRecord args = new BufferedJsonRecord ();
		args.add(Adapter.LOCATION_NAME, new JsonString(location.toUri().toString()));
		BufferedJsonRecord options = new BufferedJsonRecord();
		options.add(new JsonString("schema"), new JsonSchema(config.getDataSchema(dataField)));
		args.add(Adapter.OPTIONS_NAME, options);
	    args.add(Adapter.TYPE_NAME, new JsonString(type));
	    
	    OutputAdapter outAdapter = (OutputAdapter) JaqlUtil.getAdapterStore().output.getAdapter(args);
	    outAdapter.init(args);
	    outAdapter.open();
	    
	    if("memory".equals(filesystem)) {
	    	//Write test data
//			ClosableJsonWriter writer = outAdapter.getWriter();
//			for (int i = 0; i < 100; i++) {
//				JsonValue v = values[rnd.nextInt(values.length)];
//				writer.write(v);
//			}
//
//			writer.close();
//			outAdapter.close();
//			
//			//Set file to approximately the wanted number of records
//		    MemoryFileSystem fs = new MemoryFileSystem();
//		    fs.initialize(location.toUri(), null);
//		    fs.setRepeat(location,repetitions);
	    	//Write test data
			ClosableJsonWriter writer = outAdapter.getWriter();
			for (int i = 0; i < numberOfRecords; i++) {
				JsonValue v = values[rnd.nextInt(values.length)];
				writer.write(v);
			}

			writer.close();
			outAdapter.close();
		}
		else if("local".equals(filesystem)) {
			//Write test data
			ClosableJsonWriter writer = outAdapter.getWriter();
			for (int i = 0; i < numberOfRecords; i++) {
				JsonValue v = values[rnd.nextInt(values.length)];
				writer.write(v);
			}

			writer.close();
			outAdapter.close();
		}
	    
	}
	
	/*
	 * Necessary for correct head file size calculation
	 */
	public static String getType(Path p) {
		return pathType.get(p);
	}
	
	/*
	 * Necessary for correct head file size calculation
	 */
	public static String setType(Path p, String type) {
		return pathType.put(p, type);
	}
	
	public static long getSequenceHeaderSize(String type) {
		if(sequenceHeaderSize != -1) {
			return sequenceHeaderSize;
		}
		
		Path dummyLocation = new Path("memory://test/null-dummy-file");
		
	    BufferedJsonRecord args = new BufferedJsonRecord ();
	    BufferedJsonRecord outOptions = new BufferedJsonRecord ();

	    args.add(Adapter.LOCATION_NAME, new JsonString(dummyLocation.toString()));
	    args.add(new JsonString("type"), new JsonString(type));
	    outOptions.add(new JsonString("schema"), new JsonSchema(SchemaFactory.longSchema()));
	    args.add(new JsonString("options"), outOptions);
	    
	    //Creat Adapter
		try {
			OutputAdapter outAdapter = (OutputAdapter) JaqlUtil.getAdapterStore().output.getAdapter(args);
			outAdapter.init(args);
		    // open adapter
		    outAdapter.open();
		    
		    // get record writer
		    ClosableJsonWriter writer = outAdapter.getWriter();
		    writer.close();
		    outAdapter.close();
		    
		    MemoryFsFile f = (MemoryFsFile) MemoryFileSystem.root.getNode(dummyLocation);
		    sequenceHeaderSize = f.getFileStatus().getLen();
		    return sequenceHeaderSize;
		} catch (Exception e) {
			throw new RuntimeException("Could not create dummy file", e);
		}
	}
}
