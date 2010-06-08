package com.ibm.jaql.json.schema;

import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.expr.function.JsonValueParameter;
import com.ibm.jaql.lang.expr.function.JsonValueParameters;

/** Superclass for schemata with an annotation and the ability to store a constant value.
 * Most schemata are subclasses of this class. */
public abstract class AtomSchema<T extends JsonValue> extends AnnotatedSchema
{
  // -- schema parameters -------------------------------------------------------------------------
  
  public static final JsonValueParameters DEFAULT_PARAMETERS = getParameters(SchemaFactory.anySchema());
  
  protected static JsonValueParameters getParameters(Schema valueOrNullSchema)
  {
    return new JsonValueParameters(
        new JsonValueParameter(PAR_VALUE, valueOrNullSchema, null),
        new JsonValueParameter(PAR_ANNOTATION, SchemaFactory.recordOrNullSchema(), null));
  }
  
  protected T value;

  // -- constructors ------------------------------------------------------------------------------
  
  public AtomSchema(T value, JsonRecord annotation)
  {    
    super(annotation);
    this.value = value;
  }

  public AtomSchema(T value)
  {    
    this.value = value;
  }

  public AtomSchema()
  {    
  }
  
  // -- Schema methods ----------------------------------------------------------------------------
  
  @Override
  public boolean isConstant()
  {
    return value != null;
  }

  @Override
  public final T getConstant()
  {
    return value;
  }

  @Override
  public boolean hasModifiers()
  {
    return value != null || annotation != null;
  }
  
  @Override
  public abstract boolean matches(JsonValue value);
  
  

  // -- comparison --------------------------------------------------------------------------------

  @SuppressWarnings("unchecked")
  @Override
  public int compareTo(Schema other)
  {
    int c = this.getSchemaType().compareTo(other.getSchemaType());
    if (c != 0) return c;
    
    AtomSchema<T> o = (AtomSchema<T>)other;
    c = SchemaUtil.compare(this.value, o.value);
    if (c != 0) return c;
    c = SchemaUtil.compare(this.annotation, o.annotation);
    return c;
  }  
}
