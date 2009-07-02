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

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.SimpleTimeZone;
import java.util.TimeZone;

/**
 * 
 */
public class JsonDate extends JsonAtom
{
  public static final String iso8601UTCFormat = "yyyy-MM-dd'T'HH:mm:ss'Z'";

  protected static final SimpleDateFormat iso8601UTC = new SimpleDateFormat(iso8601UTCFormat);
  static {  iso8601UTC.setTimeZone(new SimpleTimeZone(0, "UTC")); }

  public static DateFormat getFormat(String formatStr)
  {
    SimpleDateFormat format = new SimpleDateFormat(formatStr); // TODO: add cache of formats
    if (formatStr.endsWith("'Z'") || formatStr.endsWith("'z'"))
    {
      TimeZone tz = new SimpleTimeZone(0, "UTC");
      format.setTimeZone(tz);
    }
    return format;
  }

  // TODO: should we store the original fields? Will we run into trouble storing the posix time?
  public long millis; // Milliseconds since 1970-01-01T00:00:00Z
  // todo: add timezone support

  /**
   * 
   */
  public JsonDate()
  {
  }

  /**
   * @param millis
   */
  public JsonDate(long millis)
  {
    this.millis = millis;
  }

  /**
   * 
   * @param dateStr
   * @param format
   */
  public JsonDate(String dateStr, DateFormat format)
  {
    set(dateStr, format);
  }

  /**
   * @param dateStr  date in iso8601 (only UTC specified by a Z right now)
   */
  public JsonDate(String dateStr)
  {
    set(dateStr, iso8601UTC);
  }

  /**
   * @param dateStr
   * @param formatStr
   */
  public JsonDate(String dateStr, String formatStr)
  {
    set(dateStr, getFormat(formatStr));
  }

  /**
   * @param dateStr
   */
  public JsonDate(JsonString dateStr)
  {
    this(dateStr.toString());
  }

  /**
   * @param dateStr
   * @param formatStr
   */
  public JsonDate(JsonString dateStr, JsonString formatStr)
  {
    this(dateStr.toString(), formatStr.toString());
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.json.type.JValue#getEncoding()
   */
  @Override
  public JsonEncoding getEncoding()
  {
    return JsonEncoding.DATE_MSEC;
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
    long m2 = ((JsonDate) x).millis;
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
    return JsonLong.longHashCode(millis);
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
  public void setCopy(JsonValue jvalue) throws Exception
  {
    JsonDate d = (JsonDate) jvalue;
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

  /**
   * 
   * @param dateStr
   * @param format
   */
  public void set(String dateStr, DateFormat format)
  {
    try
    {
      synchronized (format) // TODO: write our own parser code that is thread safe? 
      {
        // FIXME: add timezone support
        this.millis = format.parse(dateStr).getTime();
      }
    }
    catch (java.text.ParseException ex)
    {
      throw new java.lang.reflect.UndeclaredThrowableException(ex);
    }
  }
  
  /**
   * 
   * @param dateStr date string in iso8601 UTC (using Z) format
   */
  public void set(String dateStr)
  {
    set(dateStr, iso8601UTC);
  }
}
