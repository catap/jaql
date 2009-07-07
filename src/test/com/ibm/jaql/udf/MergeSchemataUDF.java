/**
 * 
 */
package com.ibm.jaql.udf;

import com.ibm.jaql.json.schema.SchemaTransformation;
import com.ibm.jaql.json.type.JsonSchema;

public class MergeSchemataUDF
{
  public JsonSchema eval(JsonSchema s1, JsonSchema s2)
  {
    return new JsonSchema(SchemaTransformation.merge(s1.getSchema(), s2.getSchema()));
  }
}