package com.ibm.jaql.io.serialization;

import com.ibm.jaql.json.type.JValue;

/** Superclass for serializers of <code>JValue</code>s of known type and encoding. Each 
 * <code>BasicSerializer</code> is associated with a particular implementing class <code>C</code> 
 * of {@link JValue}. It can only read and write values of type <code>C</code>. Moreover,
 * values that are read have to be written by the same <code>BasicSerializer</code>. 
 * 
 * See {@link FullSerializer} serializers that extract type information from the input.
 * 
 */
public abstract class BasicSerializer<T extends JValue> extends AbstractSerializer<T>
{
  /** Creates a new instance of the value corresponding to this serializer. */
  public abstract T newInstance();
}
