/*
 * Copyright (C) IBM Corp. 2008.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.ibm.jaql.json.type;

/**
 * 
 */
public class JsonBool extends JsonAtom
{
  // TODO: should be immutable
  public final static JsonBool TRUE  = new JsonBool(true);
  public final static JsonBool FALSE = new JsonBool(false);

  /**
   * @param tf
   * @return
   */
  public static JsonBool make(boolean tf)
  {
    return tf ? TRUE : FALSE;
  }

  public boolean value;

  /**
   * 
   */
  public JsonBool()
  {
  }

  /**
   * @param value
   */
  public JsonBool(boolean value)
  {
    this.value = value;
  }

  /**
   * @return
   */
  public boolean getValue()
  {
    return value;
  }

  /**
   * @param value
   */
  public void setValue(boolean value)
  {
    this.value = value;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JValue#getEncoding()
   */
  public JsonEncoding getEncoding()
  {
    return JsonEncoding.BOOLEAN;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JValue#compareTo(java.lang.Object)
   */
  public int compareTo(Object x)
  {
    //    int c = Util.typeCompare(this, (Writable)x);
    //    if( c != 0 )
    //    {
    //      return c;
    //    }
    boolean value2 = ((JsonBool) x).value;
    return (value == value2) ? 0 : (value2 ? -1 : +1);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JValue#longHashCode()
   */
  @Override
  public long longHashCode()
  {
    return value ? 1 : 0;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JValue#copy(com.ibm.jaql.json.type.JValue)
   */
  @Override
  public void setCopy(JsonValue jvalue) throws Exception
  {
    JsonBool b = (JsonBool) jvalue;
    value = b.value;
  }


}
