package com.ibm.jaql.udf;

import com.ibm.jaql.json.schema.ArraySchema;
import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.type.BufferedJsonArray;
import com.ibm.jaql.json.type.JsonArray;
import com.ibm.jaql.json.type.JsonLong;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.expr.agg.JavaUda;
import com.ibm.jaql.lang.expr.number.PowFn;
import com.ibm.jaql.lang.util.JaqlUtil;

/** A user-defined aggregate function that compute the average of an array of non-null long 
 * values. Optionally, each input value can be raised to a (long) power. 
 */
public class LongAvgUda implements JavaUda
{
  private long sum;
  private long count;
  private long exponent;
  
  @Override
  public void init(JsonValue... args)
  {
    sum = 0;
    count = 0;
    if (args.length == 0)
    {
      exponent = 1;
    }
    else 
    {
      exponent = ((JsonLong)args[0]).get();
    }
  }
  
  @Override
  public void accumulate(JsonValue value)
  {
    sum += PowFn.pow(((JsonLong)value).get(), exponent);
    ++count;
  }

  @Override
  public JsonValue getPartial()
  {
    BufferedJsonArray partial = new BufferedJsonArray(2);
    partial.set(0, new JsonLong(sum));
    partial.set(1, new JsonLong(count));
    return partial;
  }

  @Override
  public Schema getPartialSchema()
  {
    return new ArraySchema(new Schema[] { SchemaFactory.longSchema(), SchemaFactory.longSchema() });    
  }
  
  @Override
  public void combine(JsonValue partial)
  {
    JsonArray partialArray = (JsonArray)partial;
    try
    {
      sum += ((JsonLong)partialArray.get(0)).get();
      count += ((JsonLong)partialArray.get(1)).get();
    } catch (Exception e)
    {
      JaqlUtil.rethrow(e);
    }
  }

  @Override
  public JsonValue getFinal()
  {
    if (count == 0)
    {
      return null;
    }
    else
    {
      return new JsonLong(sum/count);
    }
  }
  
  @Override
  public Schema getFinalSchema()
  {
    return SchemaFactory.longOrNullSchema();    
  }
}
