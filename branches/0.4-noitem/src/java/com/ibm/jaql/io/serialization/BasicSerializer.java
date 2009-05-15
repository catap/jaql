package com.ibm.jaql.io.serialization;

import com.ibm.jaql.json.type.JsonValue;

/** Superclass for serializers of <code>JValue</code>s of known type and encoding. Each 
 * <code>BasicSerializer</code> is associated with a particular implementing class <code>C</code> 
 * of {@link JsonValue}. It can only read and write values of type <code>C</code>. Moreover,
 * values that are read have to be written by the same <code>BasicSerializer</code>. 
 * 
 * See {@link FullSerializer} serializers that extract type information from the input.
 * 
 */
public abstract class BasicSerializer<T extends JsonValue> extends AbstractSerializer<T>
{
  /** Creates a new instance of the value corresponding to this serializer. */
  public abstract T newInstance();
}
