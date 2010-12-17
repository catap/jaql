package com.ibm.jaql.io.hadoop;

import com.ibm.jaql.io.hadoop.DefaultHadoopInputAdapter;
import com.ibm.jaql.io.hadoop.JsonHolder;
import com.ibm.jaql.io.hadoop.JsonHolderTempKey;
import com.ibm.jaql.io.hadoop.JsonHolderTempValue;
import com.ibm.jaql.json.schema.ArraySchema;
import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonSchema;
import com.ibm.jaql.json.type.JsonString;

public class PerfHadoopInputAdapter<K, V> extends DefaultHadoopInputAdapter<K, V> {
	
	  Schema schema;
	  
	  @Override
	  protected void initializeFrom(JsonRecord args) throws Exception
	  {
	    super.initializeFrom(args);
	    
	    // check for schema option
	    JsonSchema v = (JsonSchema)options.get(new JsonString("schema"));
	    if (!(v instanceof JsonSchema))
	    {
	      throw new IllegalArgumentException("\"schema\" option not present or of invalid type");
	    }
	    schema = v.get();
	  }
	  
	  @Override
	  protected JsonHolder keyHolder()
	  {
	    return new JsonHolderPerfKey();
	  }

	  @Override
	  protected JsonHolder valueHolder()
	  {
	    return new JsonHolderPerfValue();
	  }
	  
	  @Override
	  public Schema getSchema()
	  {
	    return new ArraySchema(null, schema);
	  }
}
