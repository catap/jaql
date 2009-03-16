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

/**
 * 
 */
public class JDate extends JAtom
{
  protected static final java.text.SimpleDateFormat iso8601UTC = new java.text.SimpleDateFormat(
                                                                   "yyyy-MM-dd'T'HH:mm:ss'Z'");

  public long                                       millis;
  // todo: add timezone support

  /**
   * 
   */
  public JDate()
  {
  }

  /**
   * @param millis
   */
  public JDate(long millis)
  {
    this.millis = millis;
  }

  /**
   * @param s
   */
  public JDate(String s)
  {
    try
    {
      synchronized (iso8601UTC) // TODO: write our own parser code that is thread safe 
      {
        // TODO: timezone support
        this.millis = iso8601UTC.parse(s).getTime();
      }
    }
    catch (java.text.ParseException ex)
    {
      throw new java.lang.reflect.UndeclaredThrowableException(ex);
    }
  }

  /**
   * @param str
   */
  public JDate(JString str)
  {
    this(str.toString());
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JValue#getEncoding()
   */
  @Override
  public Item.Encoding getEncoding()
  {
    return Item.Encoding.DATE_MSEC;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JValue#compareTo(java.lang.Object)
   */
  @Override
  public int compareTo(Object x)
  {
    //    int c = Util.typeCompare(this, (Writable)x);
    //    if( c != 0 )
    //    {
    //      return c;
    //    }
    long m2 = ((JDate) x).millis;
    return (millis == m2) ? 0 : (millis < m2 ? -1 : +1);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JValue#longHashCode()
   */
  @Override
  public long longHashCode()
  {
    return JLong.longHashCode(millis);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JValue#readFields(java.io.DataInput)
   */
  @Override
  public void readFields(DataInput in) throws IOException
  {
    millis = in.readLong();
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JValue#write(java.io.DataOutput)
   */
  @Override
  public void write(DataOutput out) throws IOException
  {
    out.writeLong(millis);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JValue#toJSON()
   */
  @Override
  public String toJSON()
  {
    return "d'" + toString() + "'"; // TODO: switch to constructor form
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JValue#toString()
   */
  @Override
  public String toString()
  {
    synchronized (iso8601UTC) // TODO: write our own thread-safe formatter
    {
      return iso8601UTC.format(millis);
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JValue#copy(com.ibm.jaql.json.type.JValue)
   */
  @Override
  public void copy(JValue jvalue) throws Exception
  {
    JDate d = (JDate) jvalue;
    millis = d.millis;
  }

  /**
   * @return
   */
  public long getMillis()
  {
    return millis;
  }

  /**
   * @param millis
   */
  public void setMillis(long millis)
  {
    this.millis = millis;
  }
}
