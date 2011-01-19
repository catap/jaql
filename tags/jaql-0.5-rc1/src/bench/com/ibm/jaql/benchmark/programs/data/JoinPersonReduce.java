package com.ibm.jaql.benchmark.programs.data;

import com.ibm.jaql.benchmark.JsonConverter;
import com.ibm.jaql.benchmark.SchemaDescription;
import com.ibm.jaql.benchmark.programs.data.generator.JoinPersonReduceGenerator;
import com.ibm.jaql.json.schema.ArraySchema;
import com.ibm.jaql.json.schema.RecordSchema;
import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.type.JsonArray;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.util.JsonIterator;

public class JoinPersonReduce implements JsonConverter, SchemaDescription {
	
	private Object joinKey;
	private F[] valuesA;
	private L[] valuesB;
	
	public JoinPersonReduce() {
		
	}
	
	public Object getJoinKey() {
		return joinKey;
	}

	public F[] getValuesA() {
		return valuesA;
	}

	public L[] getValuesB() {
		return valuesB;
	}

	public static class F {
		private Person p;
		
		public void setPerson(Person p) {
			this.p = p;
		}
		
		public Person getPerson() {
			return p;
		}
	}
	
	public static class L {
		private Person p;
		
		public Person getPerson() {
			return p;
		}
	}
	
	public static class JoinResult {
		private int idA;
		private int idB;
		
		public int getIdA() {
			return idA;
		}
		public void setIdA(int idA) {
			this.idA = idA;
		}
		public int getIdB() {
			return idB;
		}
		public void setIdB(int idB) {
			this.idB = idB;
		}
		
	}

	@Override
	public Object convert(JsonValue val) {
		JsonString f = new JsonString("f");
		JsonString l = new JsonString("l");
		try {
			JsonConverter personConverter = new Person();
			
			JsonArray input = (JsonArray) val;
			JsonIterator iter = input.iter();
			iter.moveNext();
			JsonValue key = iter.current();
			iter.moveNext();
			JsonArray a = (JsonArray) iter.current();
			iter.moveNext();
			JsonArray b = (JsonArray) iter.current();
			
			JoinPersonReduce data = new JoinPersonReduce();
			data.joinKey = key.toString();
			data.valuesA = new F[(int) a.count()];
			data.valuesB = new L[(int) b.count()];
			
			for (int i = 0; i < data.valuesA.length; i++) {
				data.valuesA[i] = new F();
				data.valuesA[i].p = (Person) personConverter.convert(((JsonRecord)a.get(i)).get(f));
			}
			
			for (int i = 0; i < data.valuesB.length; i++) {
				data.valuesB[i] = new L();
				data.valuesB[i].p = (Person) personConverter.convert(((JsonRecord)b.get(i)).get(l));
			}
			
			return data;
		} catch (Exception ex) {
			throw new RuntimeException("Error: Could not convert value", ex);
		}
	}

	@Override
	public Schema getSchema() {
		Schema personSchema = new Person().getSchema();
		return new ArraySchema(new Schema[] {
			SchemaFactory.stringSchema(),
			new ArraySchema(null, new RecordSchema(new RecordSchema.Field[] {
					new RecordSchema.Field(JoinPersonReduceGenerator.F, personSchema, false)
			}, null)),
			new ArraySchema(null, new RecordSchema(new RecordSchema.Field[] {
					new RecordSchema.Field(JoinPersonReduceGenerator.L, personSchema, false)
			}, null)),
		});
	}
}
