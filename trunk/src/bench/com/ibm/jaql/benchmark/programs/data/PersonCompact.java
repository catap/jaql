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

public class PersonCompact implements JsonConverter, SchemaDescription {
	private static final JsonString LASTNAME = new JsonString("lastname");
	private static final JsonString ID = new JsonString("id");
	
	private String lastname;
	private int id;
	
	
	public PersonCompact() {
		
	}
	
	public PersonCompact(String lastname, int id) {
		this.lastname = lastname;
		this.id = id;
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
		PersonCompact p = new PersonCompact();
		p.setId(((JsonNumber)rec.get(PersonCompact.ID)).intValue());
		p.setLastname(((JsonString)rec.get(PersonCompact.LASTNAME)).toString());
		return p;
	}

	@Override
	public Schema getSchema() {
		RecordSchema.Field[] fields = new RecordSchema.Field[2];
		fields[0] = new RecordSchema.Field(PersonCompact.LASTNAME, SchemaFactory.stringSchema(), false);
		fields[1] = new RecordSchema.Field(PersonCompact.ID, SchemaFactory.longSchema(), false);
		return new RecordSchema(fields, null);
	}
}
