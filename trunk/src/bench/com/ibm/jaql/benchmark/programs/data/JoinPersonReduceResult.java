package com.ibm.jaql.benchmark.programs.data;

import com.ibm.jaql.benchmark.SchemaDescription;
import com.ibm.jaql.json.schema.ArraySchema;
import com.ibm.jaql.json.schema.RecordSchema;
import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.type.JsonString;

public class JoinPersonReduceResult implements SchemaDescription {
	public static final JsonString ID_A = new JsonString("idA");
	public static final JsonString ID_B = new JsonString("idB");
	@Override
	public Schema getSchema() {
		return new ArraySchema(null, new RecordSchema(new RecordSchema.Field[] {
				new RecordSchema.Field(ID_A, SchemaFactory.longSchema(), false),
				new RecordSchema.Field(ID_B, SchemaFactory.longSchema(), false)
		}, null));
	}

}
