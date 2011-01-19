package com.ibm.jaql.benchmark.programs.data;

import com.ibm.jaql.benchmark.SchemaDescription;
import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;

public class MixedTypes implements SchemaDescription {

	@Override
	public Schema getSchema() {
		return SchemaFactory.anySchema();
	}

}
