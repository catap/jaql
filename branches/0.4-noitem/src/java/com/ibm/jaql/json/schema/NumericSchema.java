package com.ibm.jaql.json.schema;

import com.ibm.jaql.json.type.JsonNumeric;
import com.ibm.jaql.json.type.JsonValue;

public abstract class NumericSchema<T extends JsonNumeric> extends Schema
{
  protected final T min;
  protected final T max;

  // TODO: precision

  public NumericSchema()
  {
    min = null;
    max = null;
  }
  
  public NumericSchema(T min, T max)
  {
    // check arguments
    if (!SchemaUtil.checkInterval(min, max))
    {
      throw new IllegalArgumentException("invalid interval: " + min + " " + max);
    }
    
    this.min = min;
    this.max = max;
  }
  
  public abstract boolean matches(JsonValue value);
  
  public final T getMin()
  {
    return min;
  }
  
  public final T getMax()
  {
    return max;
  }
}
