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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.PrintStream;


// import org.apache.hadoop.io.WritableComparable;

/** Superclass for all JSON values. Provides abstract methods for serialization, conversion 
 * to JSON language, deep copying, and hashing. */
public abstract class JValue implements Comparable<Object> //extends WritableComparable
{

  /**
   * @return
   */
  public abstract Item.Encoding getEncoding();

  /**
   * copy a value. value must have the same encoding as this object.
   * 
   * @param jvalue
   * @throws Exception
   */
  public abstract void copy(JValue jvalue) throws Exception;

  /*
   * (non-Javadoc)
   * 
   * @see java.lang.Comparable#compareTo(java.lang.Object)
   */
  public abstract int compareTo(Object obj);

  /*
   * (non-Javadoc)
   * 
   * @see java.lang.Object#equals(java.lang.Object)
   */
  @Override
  public boolean equals(Object x)
  {
    return this.compareTo(x) == 0;
  }

  /**
   * @param in
   * @throws IOException
   */
  public abstract void readFields(DataInput in) throws IOException;
  /**
   * @param out
   * @throws IOException
   */
  public abstract void write(DataOutput out) throws IOException;

  /**
   * Print the value on the stream in (extended) JSON text format.
   * 
   * @param out
   * @throws Exception
   */
  public abstract void print(PrintStream out) throws Exception;

  /**
   * Print the value on the stream in (extended) JSON text format. Nested items
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
   * Convert the value to a string in (extended) JSON text format.
   */
  public abstract String toJSON();

  /**
   * @return
   */
  public abstract long longHashCode();

  /**
   * Convert the value to a Java String. The default is the JSON string, but
   * some classes will override to return other strings.
   */
  @Override
  public String toString()
  {
    return toJSON();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.lang.Object#hashCode()
   */
  @Override
  public int hashCode()
  {
    return (int) (longHashCode() >>> 32);
  }
}
