package com.ibm.jaql.benchmark.programs.data;

import com.ibm.jaql.benchmark.SchemaDescription;
import com.ibm.jaql.json.schema.ArraySchema;
import com.ibm.jaql.json.schema.RecordSchema;
import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.type.JsonString;

public class JoinPersonMapResult implements SchemaDescription {
	private static final JsonString F = new JsonString("f");

	@Override
	public Schema getSchema() {
		
		RecordSchema.Field[] fields = new RecordSchema.Field[1];
		fields[0] = new RecordSchema.Field(JoinPersonMapResult.F, new Person().getSchema() , false);
		RecordSchema rec = new RecordSchema(fields, null);
		return new ArraySchema(new Schema[] {SchemaFactory.stringSchema(), rec});
	}

}
