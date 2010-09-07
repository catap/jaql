package com.ibm.jaql.benchmark.fs;

import java.util.HashMap;
import java.util.HashSet;
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
	private static HashSet<PrepareSettings> preparedFiles = new HashSet<PrepareSettings>();
	
	/* Only accepts absolute paths */
	public static void prepareRead(Path location, String type, String filesystem, JsonString dataField, JsonRecord conf) throws Exception {
		PrepareSettings settings = new PrepareSettings(location, type, filesystem, dataField, conf);
		/*
		 * Check whether the file was already prepared. The rule is that they
		 * are only prepared once for a complete benchmark run. For Jaql benchmarks
		 * this method is called several times. Creating a new file every time could
		 * destroy cache effects that would make comparisons with the JSON or Java
		 * benchmarks invalid
		 */
		//TODO: Check if it works
		if(preparedFiles.contains(settings)) {
			//Already prepared no work necessary
			return;
		} else {
			preparedFiles.add(settings);
		}
		
		BenchmarkConfig config = BenchmarkConfig.parse(conf);
		FsUtil.setType(location, type);
		Random rnd = new Random();
		rnd.setSeed(1988);
		long numberOfRecords = config.getNumberOfRecords(dataField);
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
	
	private static class PrepareSettings {
		private Path location;
		private String type;
		private String filesystem;
		private JsonString dataField;
		private JsonRecord conf;
		
		public PrepareSettings(Path location, String type, String filesystem,
				JsonString dataField, JsonRecord conf) {
			this.location = location;
			this.type = type;
			this.filesystem = filesystem;
			this.dataField = dataField;
			this.conf = conf;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((conf == null) ? 0 : conf.hashCode());
			result = prime * result
					+ ((dataField == null) ? 0 : dataField.hashCode());
			result = prime * result
					+ ((filesystem == null) ? 0 : filesystem.hashCode());
			result = prime * result
					+ ((location == null) ? 0 : location.hashCode());
			result = prime * result + ((type == null) ? 0 : type.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			PrepareSettings other = (PrepareSettings) obj;
			if (conf == null) {
				if (other.conf != null)
					return false;
			} else if (!conf.equals(other.conf))
				return false;
			if (dataField == null) {
				if (other.dataField != null)
					return false;
			} else if (!dataField.equals(other.dataField))
				return false;
			if (filesystem == null) {
				if (other.filesystem != null)
					return false;
			} else if (!filesystem.equals(other.filesystem))
				return false;
			if (location == null) {
				if (other.location != null)
					return false;
			} else if (!location.equals(other.location))
				return false;
			if (type == null) {
				if (other.type != null)
					return false;
			} else if (!type.equals(other.type))
				return false;
			return true;
		}		
	}
}
