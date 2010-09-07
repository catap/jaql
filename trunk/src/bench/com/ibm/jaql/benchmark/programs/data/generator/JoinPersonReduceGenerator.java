package com.ibm.jaql.benchmark.programs.data.generator;

import java.util.Random;

import com.ibm.jaql.benchmark.DataGenerator;
import com.ibm.jaql.benchmark.SchemaDescription;
import com.ibm.jaql.benchmark.programs.data.Person;
import com.ibm.jaql.json.schema.ArraySchema;
import com.ibm.jaql.json.schema.RecordSchema;
import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.type.BufferedJsonArray;
import com.ibm.jaql.json.type.BufferedJsonRecord;
import com.ibm.jaql.json.type.JsonArray;
import com.ibm.jaql.json.type.JsonLong;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.util.FieldNameCache;

public class JoinPersonReduceGenerator implements DataGenerator, SchemaDescription {

	public static final JsonString F = FieldNameCache.get(new JsonString("f"));
	public static final JsonString L = FieldNameCache.get(new JsonString("l"));
	private static final JsonString LASTNAME = FieldNameCache.get(new JsonString("lastname"));
	private static final JsonString FORENAME = FieldNameCache.get(new JsonString("forename"));
	private static final JsonString ID = FieldNameCache.get(new JsonString("id"));
	private Random rnd = new Random(1988);
	
	private static final String[] forenames = new String[] { "Herbert", "Robert", "Julia", 
			"Klaus", "Thomas", "Janine", "Anna", "Michelle", "Dirk", "Heidi", "Max" };
	private static final String[] lastnames = new String[] { "Langer", "Ballatre", "Ebel",
			"Öttel", "Zucha", "Zaiser", "Büche", "Schäffler", "Biesinger", "Mohn"};
	
	@Override
	public JsonValue generate() {
		JsonString lastname = generateRandomLastname();
		JsonArray arrA = generateJoinArrays(lastname, F);
		JsonArray arrB = generateJoinArrays(lastname, L);
		
		JsonArray joinArr = new BufferedJsonArray(new JsonValue[] {lastname, arrA, arrB }, false);
		return joinArr;
	}

	private JsonArray generateJoinArrays(JsonString lastname, JsonString boxField) {
		int size = rnd.nextInt(4)+1;
		JsonValue[] values = new JsonValue[size];
		for (int i = 0; i < values.length; i++) {
			values[i] = boxPersonRecord(boxField, generatePersonRecord(lastname));
		}
		return new BufferedJsonArray(values, false);
	}
	
	private BufferedJsonRecord generatePersonRecord(JsonString lastname) {
		BufferedJsonRecord rec = new BufferedJsonRecord(3);
		rec.add(LASTNAME, lastname);
		rec.add(FORENAME, getRandomForename());
		rec.add(ID, getRandomID());
		return rec;
	}
	
	private BufferedJsonRecord boxPersonRecord(JsonString field, JsonRecord val) {
		BufferedJsonRecord rec = new BufferedJsonRecord(1);
		rec.add(field, val);
		return rec;
	}

	private JsonString getRandomForename() {
		return new JsonString(forenames[rnd.nextInt(forenames.length)]);
	}
	
	private JsonLong getRandomID() {
		return new JsonLong(rnd.nextInt(100)+100);
	}
	
	private JsonString generateRandomLastname() {
		return new JsonString(lastnames[rnd.nextInt(lastnames.length)]);
	}

	@Override
	public Schema getSchema() {
		Person p = new Person();
		return new ArraySchema(new Schema[] {
				SchemaFactory.stringSchema(),
				new ArraySchema(null, new RecordSchema(new RecordSchema.Field[] {
						new RecordSchema.Field(F, p.getSchema(), false)
				}, null)),
				new ArraySchema(null, new RecordSchema(new RecordSchema.Field[] {
						new RecordSchema.Field(L, p.getSchema(), false)
				}, null))
		});
	}
}
