package com.ibm.jaql.benchmark.programs.data;

import com.ibm.jaql.benchmark.JsonConverter;
import com.ibm.jaql.benchmark.SchemaDescription;
import com.ibm.jaql.json.schema.RecordSchema;
import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.type.JsonNumber;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;

public class Person implements JsonConverter, SchemaDescription {
	private static final JsonString FORENAME = new JsonString("forename");
	private static final JsonString LASTNAME = new JsonString("lastname");
	private static final JsonString ID = new JsonString("id");
	
	private String forename;
	private String lastname;
	private int id;
	
	
	public Person() {
		
	}
	
	public Person(String forename, String lastname, int id) {
		this.forename = forename;
		this.lastname = lastname;
		this.id = id;
	}
	
	public String getForename() {
		return forename;
	}
	public void setForename(String forename) {
		this.forename = forename;
	}
	public String getLastname() {
		return lastname;
	}
	public void setLastname(String lastname) {
		this.lastname = lastname;
	}
	public int getId() {
		return id;
	}
	public void setId(int id) {
		this.id = id;
	}

	@Override
	public Object convert(JsonValue val) {		
		JsonRecord rec = (JsonRecord) val;
		Person p = new Person();
		p.setId(((JsonNumber)rec.get(Person.ID)).intValue());
		p.setForename(((JsonString)rec.get(Person.FORENAME)).toString());
		p.setLastname(((JsonString)rec.get(Person.LASTNAME)).toString());
		return p;
	}

	@Override
	public Schema getSchema() {
		RecordSchema.Field[] fields = new RecordSchema.Field[3];
		fields[0] = new RecordSchema.Field(Person.FORENAME, SchemaFactory.stringSchema(), false);
		fields[1] = new RecordSchema.Field(Person.LASTNAME, SchemaFactory.stringSchema(), false);
		fields[2] = new RecordSchema.Field(Person.ID, SchemaFactory.longSchema(), false);
		return new RecordSchema(fields, null);
	}
}
