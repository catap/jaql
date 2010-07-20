package com.ibm.jaql.benchmark.programs.data;

import com.ibm.jaql.benchmark.SchemaDescription;
import com.ibm.jaql.json.schema.RecordSchema;
import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.schema.RecordSchema.Field;
import com.ibm.jaql.json.type.JsonString;

public class Transition implements SchemaDescription {

	@Override
	public Schema getSchema() {
		Field[] fields = new RecordSchema.Field[4];
		fields[0] = new RecordSchema.Field(new JsonString("MCC1"), SchemaFactory.longSchema(), true);
		fields[1] = new RecordSchema.Field(new JsonString("ZIP1"), SchemaFactory.longSchema(), true);
		fields[2] = new RecordSchema.Field(new JsonString("MCC2"), SchemaFactory.longSchema(), false);
		fields[3] = new RecordSchema.Field(new JsonString("ZIP2"), SchemaFactory.longSchema(), false);
		return new RecordSchema(fields, null);
	}

}
