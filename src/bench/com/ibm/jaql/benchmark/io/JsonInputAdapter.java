package com.ibm.jaql.benchmark.io;

import java.io.IOException;
import java.util.Random;

import com.ibm.jaql.benchmark.util.BenchmarkConfig;
import com.ibm.jaql.io.ClosableJsonIterator;
import com.ibm.jaql.io.InputAdapter;
import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;

//TODO: Check that records are not changed during run
public class JsonInputAdapter implements InputAdapter {
	long numberOfRecords = -1;
	JsonValue[] testData = null;
	boolean copyRecords = false;
	
	@Override
	/**
	 * Takes a benchmark configuration record as base, not a input record
	 * like the default jaql input adapters
	 */
	@Deprecated
	public void init(JsonValue options) throws Exception {		
		throw new RuntimeException("This input adapter should be initialized using the init(JsonString dataField, JsonRecord conf) method");
	}
	
	public void init(JsonString dataField, JsonRecord conf) throws Exception {
		//Read number of records to simulate
		numberOfRecords = BenchmarkConfig.parse(conf).getNumberOfRecords(dataField);
		
		/* 
		 * Read data that should be used to generate the records and copy it
		 * into an internal array for fast access
		 */
		testData = BenchmarkConfig.parse(conf).getData(dataField);
	}

	@Override
	public ClosableJsonIterator iter() throws Exception {
		return new ClosableJsonIterator() {
			JsonValue val = null;
			int countRecord = 0;
			final boolean copy = copyRecords;
			Random rnd = new Random(1988);

			@Override
			public void close() throws IOException {
			}

			@Override
			public boolean moveNext() throws IOException {
				//AbstractBenchmark.TIMER.pause();
				countRecord++;
				if (countRecord > numberOfRecords) {
					return false;
				}
				
				try {
					//TODO: Get better understanding of mutable and unmutable
					if(copy) {
						currentValue = testData[rnd.nextInt(testData.length)].getCopy(val);
					} else {
						currentValue = testData[rnd.nextInt(testData.length)];
					}
				} catch (Exception e) {
					throw new IOException("Could not copy record", e);
				}
				//AbstractBenchmark.TIMER.resume();
				return true;
			}
		};
	}
	
	@Override
	public Schema getSchema() {
		return SchemaFactory.arraySchema();
	}
	
	@Override
	public void close() throws Exception {
	}

	@Override
	public void open() throws Exception {
	}
	
	public void setCopyRecords(boolean copy) {
		copyRecords = copy;
	}

}
