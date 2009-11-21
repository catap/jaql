package com.ibm.jaql.json.schema;

import com.ibm.jaql.json.type.JsonLong;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonUtil;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.expr.function.JsonValueParameter;
import com.ibm.jaql.lang.expr.function.JsonValueParameters;

public abstract class AtomSchemaWithLength<T extends JsonValue> extends AtomSchema<T>
{
  // -- schema parameters -------------------------------------------------------------------------
  
  public static final JsonValueParameters DEFAULT_PARAMETERS = getParameters(SchemaFactory.anySchema());
  
  protected static JsonValueParameters getParameters(Schema valueOrNullSchema)
  {
    return new JsonValueParameters(
        new JsonValueParameter(PAR_LENGTH, SchemaFactory.longOrNullSchema(), null),
        new JsonValueParameter(PAR_VALUE, valueOrNullSchema, null),
        new JsonValueParameter(PAR_ANNOTATION, SchemaFactory.recordOrNullSchema(), null));
  }
  
  
  // -- variables ---------------------------------------------------------------------------------
  
  protected JsonLong length;
  
  
  // -- constructors ------------------------------------------------------------------------------
  
  public AtomSchemaWithLength(JsonLong length, T value, JsonRecord annotation)
  {
    super(value, annotation);
    this.length = (JsonLong)JsonUtil.getImmutableCopyUnchecked(length);
    
    // do some checks
    if (length != null)
    {
      long l = length.longValue();
      if (l < 0)
      {
        throw new IllegalArgumentException("length attribute must be non-negative");
      }
      if (value != null && lengthOf(value) != l)
      {
        throw new IllegalArgumentException("length does not match length of specified value");
      }
    }
    if (value != null)
    {
      this.length = new JsonLong(lengthOf(value));
    }    
  }

  public AtomSchemaWithLength(JsonLong length, JsonRecord annotation)
  {
    this(length, null, annotation);
  }
  
  public AtomSchemaWithLength(JsonLong length)
  {
    this(length, null);
  }
  
  public AtomSchemaWithLength(T value, JsonRecord annotation)
  {
    super(value, annotation);
  }
  
  public AtomSchemaWithLength(T value)
  {
    super(value);
  }
  
  public AtomSchemaWithLength()
  {
    super();
  }
  
  
  // -- abstract methods --------------------------------------------------------------------------
  
  protected abstract long lengthOf(T value);
  
  
  // -- schema methods ----------------------------------------------------------------------------
  
  @Override
  public boolean hasModifiers()
  {
    return super.hasModifiers() || length != null;
  }

  
  // -- getters -----------------------------------------------------------------------------------
  
  public JsonLong getLength()
  {
    return length;
  }
  
  
  // -- comparison --------------------------------------------------------------------------------

  @SuppressWarnings("unchecked")
  @Override
  public int compareTo(Schema other)
  {
    int c = this.getSchemaType().compareTo(other.getSchemaType());
    if (c != 0) return c;
    
    AtomSchemaWithLength<T> o = (AtomSchemaWithLength<T>)other;
    c = SchemaUtil.compare(this.length, o.length);
    if (c != 0) return c;
    c = SchemaUtil.compare(this.value, o.value);
    if (c != 0) return c;
    c = SchemaUtil.compare(this.annotation, o.annotation);
    return c;
  } 
}
