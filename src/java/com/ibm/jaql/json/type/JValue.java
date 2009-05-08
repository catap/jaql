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

import java.io.PrintStream;


// import org.apache.hadoop.io.WritableComparable;

/** Superclass for all JSON values. Provides abstract methods for serialization, conversion 
 * to JSON language, deep copying, and hashing. */
public abstract class JValue implements Comparable<Object> //extends WritableComparable
{

  /** Returns the encoding of this object.
   * 
   * @return the encoding of this object
   */
  public abstract Item.Encoding getEncoding();

  /** Copy the content of <code>jvalue</code> into this object. The provided value must have the 
   * same encoding as this object.
   * 
   * @param jvalue a value
   * @throws Exception
   */
  public abstract void setCopy(JValue jvalue) throws Exception;


  /**
   * Print this value on the stream in (extended) JSON text format.
   * 
   * @param out
   * @throws Exception
   */
  public abstract void print(PrintStream out) throws Exception;

  /**
   * Print this value on the stream in (extended) JSON text format. Nested items
   * are indented by the indent value.
   * 
   * @param out
   * @param indent
   * @throws Exception
   */
  public void print(PrintStream out, int indent) throws Exception
  {
    print(out);
  }

  /**
   * Convert this value to a string in (extended) JSON text format.
   */
  public abstract String toJSON();

  /**
   * Convert this value to a Java String. The default is the JSON string, but
   * some classes will override to return other strings.
   */
  @Override
  public String toString()
  {
    return toJSON();
  }

  /* @see java.lang.Object#equals(java.lang.Object) */
  @Override
  public boolean equals(Object x)
  {
    return this.compareTo(x) == 0;
  }

  /* @see java.lang.Comparable#compareTo(java.lang.Object) */
  public abstract int compareTo(Object obj);

  /* @see java.lang.Object#hashCode() */
  @Override
  public int hashCode()
  {
    return (int) (longHashCode() >>> 32);
  }
  
  /** Returns a long hash code for this value.
   * 
   * @return a long hash code
   */
  public abstract long longHashCode();
}
