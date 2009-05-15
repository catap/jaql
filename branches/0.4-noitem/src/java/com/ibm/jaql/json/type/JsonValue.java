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
public abstract class JsonValue implements Comparable<Object>
{
  /** Returns the encoding of this object.
   * 
   * @return the encoding of this object
   */
  public abstract JsonEncoding getEncoding();

  /** Copy the content of <code>jvalue</code> into this object. The provided value must have the 
   * same encoding as this object.
   * 
   * @param jvalue a value
   * @throws Exception
   */
  public abstract void setCopy(JsonValue jvalue) throws Exception;

  /** Obtain a copy of this value */
  public JsonValue getCopy(JsonValue target) throws Exception {
    if (target==null || getEncoding() != target.getEncoding() || this==target)
    {
      target = getEncoding().newInstance();
    }
    target.setCopy(this);
    return target;
  }
  
  /**
   * Print this value on the stream in (extended) JSON text format.
   * 
   * @param out
   * @throws Exception
   */
  protected abstract void print(PrintStream out) throws Exception;

  /**
   * Print this value on the stream in (extended) JSON text format. Nested items
   * are indented by the indent value.
   * 
   * @param out
   * @param indent
   * @throws Exception
   */
  protected void print(PrintStream out, int indent) throws Exception
  {
    print(out);
  }

  
  /**
   * Convert this value to a string in (extended) JSON text format.
   */
  public abstract String toJson();

  /**
   * Convert this value to a Java String. The default is the JSON string, but
   * some classes will override to return other strings.
   */
  @Override
  public String toString()
  {
    return toJson();
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

  
  // -- static convenience methods that deal with null's ------------------------------------------ 
  
  // TODO: all these should go to a utility class to keep the API clean
  
  public static String toString(JsonValue value)
  {
    if (value == null) {
      return "null";
    } else {
      return value.toString();
    }
  }
  
  /**
   * Print <code>value</code>, if non-null, on the stream in (extended) JSON text format using
   * <code>v.print(out)</code>. Otherwise, prints <code>null</code>. 
   * 
   * @param value a value or <code>null</code>
   * @param out an output stream
   * @param indent indentation value
   * @throws Exception
   */
  public static void print(PrintStream out, JsonValue value) throws Exception {
    if (value == null) {
      out.print("null");
    } else {
      value.print(out);
    }
  }
  
  /**
   * Print <code>value</code>, if non-null, on the stream in (extended) JSON text format using
   * <code>v.print(out, indent)</code>. Otherwise, prints <code>null</code>. 
   * 
   * @param value a value or <code>null</code>
   * @param out an output stream
   * @param indent indentation value
   * @throws Exception
   */
  public static void print(PrintStream out, JsonValue value, int indent) throws Exception {
    if (value == null) {
      out.print("null");
    } else {
      value.print(out, indent);
    }
  }

  /** Handles null */
  public static int compare(JsonValue v1, JsonValue v2) {
    if (v1 == null) {
      return v2==null ? 0 : -1; // nulls go first
    } 
    if (v2 == null) {
      return 1;
    }
    return v1.compareTo(v2);
  }
  
  /** Handles null */
  public static long longHashCode(JsonValue v)  {
    if (v == null) {
      return 0;
    } 
    return v.longHashCode();
  }
  
  public static JsonValue getCopy(JsonValue src, JsonValue target) throws Exception
  {
    if (src == null) 
    {
      return null;
    }
    return src.getCopy(target);
  }
  
}
