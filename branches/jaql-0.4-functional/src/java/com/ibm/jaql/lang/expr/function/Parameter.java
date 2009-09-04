package com.ibm.jaql.lang.expr.function;

import java.io.IOException;

import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonUtil;

/** A formal parameter of a function 
 * 
 * @param T type of default value 
 */
public abstract class Parameter<T>
{
  public enum Type { REQUIRED, OPTIONAL, REPEATING };
  
  private final JsonString name;
  private final Schema schema;
  private final Type type;
  private final T defaultValue;
  
  
  // -- construction ------------------------------------------------------------------------------
  
  public Parameter(JsonString name, Schema schema, T defaultValue)
  {
    if (name == null) throw new IllegalArgumentException("parameter name cannot be null");
    this.name = (JsonString)JsonUtil.getImmutableCopyUnchecked(name);
    this.schema = schema != null ? schema : SchemaFactory.anySchema();
    this.type = Type.OPTIONAL;
    this.defaultValue = processDefault(defaultValue);
  }
  
  public Parameter(String name, Schema schema, T defaultValue)
  {
    this(new JsonString(name), schema, defaultValue);
  }
  
  
  public Parameter(JsonString name, String schema, T defaultValue) 
  {
    if (name == null) throw new IllegalArgumentException("parameter name cannot be null");
    this.name = (JsonString)JsonUtil.getImmutableCopyUnchecked(name);
    try
    {
      this.schema = schema != null ? SchemaFactory.parse(schema) : SchemaFactory.anySchema();
    } catch (IOException e)
    {
      throw new IllegalArgumentException(e);
    }
    this.type = Type.OPTIONAL;
    this.defaultValue = processDefault(defaultValue);
  }
  
  public Parameter(String name, String schema, T defaultValue)
  {
    this(new JsonString(name), schema, defaultValue);
  }
  
  
  public Parameter(JsonString name, Schema schema, boolean isRepeating)
  {
    if (name == null) throw new IllegalArgumentException("parameter name cannot be null");
    this.name = (JsonString)JsonUtil.getImmutableCopyUnchecked(name);
    this.schema = schema != null ? schema : SchemaFactory.anySchema();
    this.type = isRepeating ? Type.REPEATING : Type.REQUIRED;
    this.defaultValue = null;
  }
  
  public Parameter(String name, Schema schema, boolean isRepeating)
  {
    this(new JsonString(name), schema, isRepeating);
  }
  
  public Parameter(JsonString name, Schema schema)
  {
    this(name, schema, false);
  }
  
  public Parameter(String name, Schema schema)
  {
    this(new JsonString(name), schema);
  }
  
  public Parameter(JsonString name, String schema)
  {
    if (name == null) throw new IllegalArgumentException("parameter name cannot be null");
    this.name = (JsonString)JsonUtil.getImmutableCopyUnchecked(name);
    try
    {
      this.schema = schema != null ? SchemaFactory.parse(schema) : SchemaFactory.anySchema();
    } catch (IOException e)
    {
      throw new IllegalArgumentException(e);
    }
    this.type = Type.REQUIRED;
    this.defaultValue = null;
  }
  
  public Parameter(String name, String schema)
  {
    this(new JsonString(name), schema);
  }
  
  public Parameter(JsonString name)
  {
    this(name, SchemaFactory.anySchema());
  }
  
  public Parameter(String name)
  {
    this(new JsonString(name));
  }
  
  /** Validate and, if necessary, copy the default value of this parameter. */
  protected abstract T processDefault(T value);
  
  
  // -- getters -----------------------------------------------------------------------------------
  
  public JsonString getName() 
  { 
    return name; 
  }
  
  public Schema getSchema()
  {
    return schema;
  }
  
  public boolean isRequired()
  {
    return type == Type.REQUIRED;
  }
  
  public boolean isOptional()
  {
    return type == Type.OPTIONAL;
  }

  public boolean isRepeating()
  {
    return type == Type.REPEATING;
  }
  
  public T getDefaultValue()
  {
    return defaultValue;
  }
}
