/**
 * 
 */
package com.ibm.jaql.udf;

import com.ibm.jaql.json.schema.SchemaTransformation;
import com.ibm.jaql.json.type.JsonSchema;

public class MergeSchemataUdf
{
  public JsonSchema eval(JsonSchema s1, JsonSchema s2)
  {
    return new JsonSchema(SchemaTransformation.merge(s1.get(), s2.get()));
  }
}