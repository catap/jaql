package com.ibm.jaql.lang.expr.function;

import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;

/** A parameters with JSON default values. */
public class JsonValueParameters extends Parameters<JsonValue>
{
  public JsonValueParameters()
  {
    super();
    // TODO Auto-generated constructor stub
  }

  public JsonValueParameters(JsonString... names)
  {
    super(names);
    // TODO Auto-generated constructor stub
  }

  public JsonValueParameters(JsonString[] names, int min, JsonValue defaultValue)
  {
    super(names, min, defaultValue);
    // TODO Auto-generated constructor stub
  }

  public JsonValueParameters(JsonValueParameter... parameters)
  {
    super(parameters);
    // TODO Auto-generated constructor stub
  }

  public JsonValueParameters(String... names)
  {
    super(names);
    // TODO Auto-generated constructor stub
  }


  @Override
  protected Parameter<JsonValue> createParameter(JsonString name, Schema schema)
  {
    return new JsonValueParameter(name, schema);
  }

  @Override
  protected Parameter<JsonValue> createParameter(JsonString name,
      Schema schema, JsonValue defaultValue)
  {
    return new JsonValueParameter(name, schema, defaultValue);
  }

  @Override
  protected JsonValue[] newArrayOfT(int size)
  {
    return new JsonValue[size];
  }
  
  // -- utils -------------------------------------------------------------------------------------
 
  /** If <code>args</code> is non-null and contains a field of name <code>parName</code>, returns
   * the value of this field. Otherwise, returns the default value of the specified parameter
   * (or throws an exception, if this parameter does not have a default value). */
  public JsonValue argumentOrDefault(JsonString parName, JsonRecord args)
  {
    if (args != null && args.containsKey(parName))
    {
      return args.get(parName);
    }
    else
    {
      int position = positionOf(parName); 
      if (position < 0) throw new IllegalArgumentException();
      return defaultOf(position);
    }
  }
}
