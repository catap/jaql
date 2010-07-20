package com.ibm.jaql.benchmark.util;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.ibm.jaql.io.serialization.binary.BinaryFullSerializer;
import com.ibm.jaql.io.serialization.binary.def.DefaultBinaryFullSerializer;
import com.ibm.jaql.io.serialization.binary.perf.PerfBinaryFullSerializer;
import com.ibm.jaql.io.serialization.binary.temp.TempBinaryFullSerializer;
import com.ibm.jaql.json.schema.Schema;

public class RawSerializerUtil {

	public static BinaryFullSerializer getSerializer(String serializer, Schema s) {
		if(serializer.equalsIgnoreCase("hdfs")) {
			return DefaultBinaryFullSerializer.getInstance();
		}
		else if(serializer.equalsIgnoreCase("jaqltemp")) {
			return new TempBinaryFullSerializer(s);
		}
		else if(serializer.equalsIgnoreCase("perf")) {
			return new PerfBinaryFullSerializer(s);
		}
		throw new RuntimeException("Unknow serializer " + serializer);
	}
	
	public static FSDataInputStream createInput(Path location) throws IOException {
		FileSystem fs = location.getFileSystem(null);
		return fs.open(location);
	}
	
	public static FSDataOutputStream createOutput(Path location) throws IOException {
		FileSystem fs = location.getFileSystem(null);
		return fs.create(location, true);
	}
	
	public static Path outputLocation(String filesystem) {
		if("memory".equals(filesystem)) {
			return new Path("memory://" +  "test/null-"+System.nanoTime());
		}
		else if("local".equals(filesystem)) {
			try {
				return new Path((new File(".")).getCanonicalPath()+ "/temp/out-" + System.nanoTime());
			} catch (IOException e) {
				throw new RuntimeException("lol");
			}
		}
		throw new RuntimeException("Invalid option");
		//return new Path("memory://" +  "/test/gen/in-"+System.nanoTime());
		//TODO: Bug in fs implementation
		
	}
}
