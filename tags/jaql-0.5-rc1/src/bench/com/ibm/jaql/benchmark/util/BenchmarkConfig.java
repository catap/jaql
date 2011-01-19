package com.ibm.jaql.benchmark.util;

import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;

import javax.management.RuntimeErrorException;

import com.ibm.jaql.benchmark.DataGenerator;
import com.ibm.jaql.benchmark.JsonConverter;
import com.ibm.jaql.benchmark.SchemaDescription;
import com.ibm.jaql.json.parser.JsonParser;
import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.type.BufferedJsonRecord;
import com.ibm.jaql.json.type.JsonArray;
import com.ibm.jaql.json.type.JsonNumber;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.util.FieldNameCache;
import com.ibm.jaql.json.util.JsonIterator;

public class BenchmarkConfig {
	private static final JsonString JAQL_SCRIPT = new JsonString("jaqlscript");
	private static final JsonString JSON_BENCH = new JsonString("jsonBench");
	private static final JsonString JAVA_BENCH = new JsonString("javaBench");
	private static final JsonString RESULT_SCHEMA = new JsonString("resultSchema");
	private static final JsonString ITERATIONS = new JsonString("iterations");
	private static final JsonString NUMBER_OF_RECORDS = new JsonString("numberOfRecords");
	
	private static final JsonString DATA_SCHEMA = new JsonString("schema");
	private static final JsonString DATA_CONVERTER = new JsonString("jsonConverter");
	private static final JsonString DATA_VALUES = new JsonString("values");
	private static final JsonString DATA_GENERATOR= new JsonString("generator");
	
	JsonRecord config;
	Schema resultSchema;
	int iterations;
	long numberOfRecords;

	private BenchmarkConfig(JsonRecord conf) {
		if(!BenchmarkConfig.getConfigurationSchema().matchesUnsafe(conf)) {
			throw new RuntimeException("Invalid configuration");
		}
		
		config = conf;
		iterations = ((JsonNumber)conf.get(ITERATIONS)).intValueExact();
		
		JsonString resultSchemaClass = (JsonString) conf.get(RESULT_SCHEMA);
		if(resultSchemaClass != null) {
			SchemaDescription schemaDescription = null;
			try {
				schemaDescription = (SchemaDescription) ClassLoader.getSystemClassLoader().loadClass(
						resultSchemaClass.toString()).newInstance();
				resultSchema = schemaDescription.getSchema();
			} catch (Exception e) {
				throw new RuntimeException("Could not load schema information class", e);
			}
		}
	}
	
	public static BenchmarkConfig parse(JsonRecord conf) {
		return new BenchmarkConfig(conf);
	}
	
	public Schema getResultSchema() {
		if(resultSchema == null) throw new RuntimeException("Does not exist");
		return resultSchema;
	}
	
	public int getIterations() {
		return iterations;
	}
	
	public long getNumberOfRecords(JsonString dataField) {
		JsonNumber n = (JsonNumber) getDataRecord(dataField).get(NUMBER_OF_RECORDS);
		return n.longValueExact();
	}
	
	public String getJaqlScriptLocation() {
		JsonString jaqlScriptLocation = (JsonString)config.get(JAQL_SCRIPT);
		return jaqlScriptLocation!=null?jaqlScriptLocation.toString():null;
	}
	
	public Schema getDataSchema(JsonString dataField) {
		JsonString className = (JsonString) getDataRecord(dataField).get(BenchmarkConfig.DATA_SCHEMA);
		
		if(className == null) {
			return null;
		}
		
		SchemaDescription schema = null;
		try {
			schema = (SchemaDescription) ClassLoader.getSystemClassLoader().loadClass(className.toString()).newInstance();
		} catch (Exception e) {
			throw new RuntimeException("Could not load schema information class", e);
		}
		
		return schema.getSchema();
	}
	
	public JsonConverter getDataConverter(JsonString dataField) {
		JsonString converterClass = (JsonString) getDataRecord(dataField).get(DATA_CONVERTER);
		
		if(converterClass == null) {
			return null;
			
		}
		
		JsonConverter converter = null;
		try {
			converter = (JsonConverter) ClassLoader.getSystemClassLoader().loadClass(converterClass.toString()).newInstance();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		return converter;
	}
	
	public JsonValue[] getData(JsonString dataField) throws Exception {
		JsonRecord dataRecord = getDataRecord(dataField);
		
		JsonArray data = (JsonArray) dataRecord.get(BenchmarkConfig.DATA_VALUES);
		
		JsonValue[] values = null;
		/* Values are directly given */
		if(data != null) {
			values = new JsonValue[(int) data.count()];
			
			JsonIterator iter =	((JsonArray)data).iter();
			int i = 0;
			for (JsonValue d : iter) {
				if(d != null) {
					values[i] = d.getImmutableCopy();
					if(values[i] instanceof BufferedJsonRecord) {
						//Convert names for optimized field access
						JsonString[] names = ((BufferedJsonRecord)values[i]).getInternalNamesArray();
						for (int j = 0; j < names.length; j++) {
							names[j] = FieldNameCache.get((JsonString) names[j]);
						}
						//TODO: Recalculate hash index
						((BufferedJsonRecord)values[i]).set(names, ((BufferedJsonRecord)values[i]).getInternalValuesArray(), ((BufferedJsonRecord)values[i]).size());
					}
					i++;
				} else {
					values[i] = d;
				}
			}
		} 
		/* Values need to be generated */
		else {
			values = new JsonValue[100];
			JsonString gen = (JsonString) dataRecord.get(BenchmarkConfig.DATA_GENERATOR);
			DataGenerator generator = null;
			try {
				generator = (DataGenerator) ClassLoader.getSystemClassLoader().loadClass(gen.toString()).newInstance();
			} catch (Exception e) {
				throw new RuntimeException("Could not load output schema information class", e);
			}
			
			for (int i = 0; i < values.length; i++) {
				values[i] = generator.generate();
				if(values[i] instanceof BufferedJsonRecord) {
					//Convert names for optimized field access
					JsonString[] names = ((BufferedJsonRecord)values[i]).getInternalNamesArray();
					for (int j = 0; j < names.length; j++) {
						names[j] = FieldNameCache.get((JsonString) names[j]);
					}
					((BufferedJsonRecord)values[i]).set(names, ((BufferedJsonRecord)values[i]).getInternalValuesArray(), ((BufferedJsonRecord)values[i]).size());
				}
			}
		}
		
		return values;
	}
	
	public static Schema getConfigurationSchema() {
		return SchemaFactory.anySchema();
	}
	
	private JsonRecord getDataRecord(JsonString dataField) {
		JsonValue v = config.get(dataField);
		if(v == null || !(v instanceof JsonRecord)) {
			throw new RuntimeException("Non existing or invalid data field " + dataField);
		}
		
		return (JsonRecord)v;
	}
	
	public static JsonRecord getBenchmarkRecord(String name) throws Exception {
		JsonValue value;
		
		URL url = ClassLoader.getSystemResource(name + ".json");
		// Reader r = new BufferedReader(new FileReader(name + ".json"));
		Reader r = new InputStreamReader(url.openStream());
		JsonParser parser = new JsonParser(r);
		value = parser.JsonVal();
		
		return (JsonRecord) value;
	}

	public String getJavaBenchmark() {
		return ((JsonString)config.get(JAVA_BENCH)).toString();
	}
	
	public String getJsonBenchmark() {
		return ((JsonString)config.get(JSON_BENCH)).toString();
	}

	public JsonRecord getRecord() {
		return config;
	}
}
