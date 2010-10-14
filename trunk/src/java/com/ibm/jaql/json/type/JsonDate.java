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

import java.lang.reflect.UndeclaredThrowableException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.GregorianCalendar;
import java.util.Locale;
import java.util.SimpleTimeZone;
import java.util.TimeZone;

import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;

import com.ibm.jaql.lang.util.JaqlUtil;

/** An JSON date. 
 * 
 * Instances of this class are immutable, but subclasses might add mutation functionality
 * (in which case they have to override the {@link #getCopy(JsonValue)} method).
 */
public class JsonDate extends JsonAtom
{
  protected static final TimeZone UTC = TimeZone.getTimeZone(TimeZone.getAvailableIDs(0)[0]);
  
  protected final static DatatypeFactory CAL_FACTORY;
  static
  {
    try
    {
      CAL_FACTORY = DatatypeFactory.newInstance();
    }
    catch( DatatypeConfigurationException e )
    {
      throw new UndeclaredThrowableException(e);
    }
  }
  
//  public static final String ISO8601UTC_FORMAT_STRING = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
//
//  protected static final SimpleDateFormat ISO8601UTC_FORMAT1;
//  static {  
//    ISO8601UTC_FORMAT1 = new SimpleDateFormat(ISO8601UTC_FORMAT_STRING); 
//    ISO8601UTC_FORMAT1.setTimeZone(new SimpleTimeZone(0, "UTC")); 
//  }


  // TODO: should we store the original fields? Will we run into trouble storing the posix time?
  protected long millis; // Milliseconds since 1970-01-01T00:00:00Z
  // todo: add timezone support

  
  // -- construction ------------------------------------------------------------------------------
  
  /** Constructs a new <code>JsonDate</code> using the specified timestamp.
   * 
   * @param millis number of milliseconds since January 1, 1970, 00:00:00 GMT
   */
  public JsonDate(long millis)
  {
    set(millis);
  }

  /** Constructs a new <code>JsonDate</code> using the specified value.
   * 
   * @param date string representation of the date
   * @param format formatter to parse the date
   */
  public JsonDate(String date, DateFormat format)
  {
    set(date, format);
  }

  /** Constructs a new data using the specified value.
   * 
   * @param date string representation of the data in iso8601 (only UTC specified by a Z right now) 
   */
  public JsonDate(String date)
  {
    set(date);
    // set(date, ISO8601UTC_FORMAT);
  }
  
  /** Constructs a new data using the specified value.
   * 
   * @param date string representation of the data in iso8601 (only UTC specified by a Z right now) 
   */
  public JsonDate(JsonString date)
  {
    this(date.toString());
  }
  
  /** Constructs a new data using the specified value.
   * 
   * @param date string representation of the date
   * @param format format string of the date; passed to {@link #getFormat(String)}.
   */  
  public JsonDate(String date, String format)
  {
    set(date, getFormat(format));
  }

  /** Constructs a new data using the specified value.
   * 
   * @param date string representation of the date
   * @param format format string of the date; passed to {@link #getFormat(String)}.
   */
  public JsonDate(JsonString date, JsonString format)
  {
    this(date.toString(), format.toString());
  }

  // -- getters -----------------------------------------------------------------------------------

  /** Returns the number of milliseconds since January 1, 1970, 00:00:00 GMT represented by this 
   * <code>JsonDate</code> object.
   */
  public long get()
  {
    return millis;
  }
  
  /** Returns a string representation of this date in ISO 8601 format. */
  @Override
  public String toString()
  {
    GregorianCalendar cal = new GregorianCalendar(UTC);
    cal.setTimeInMillis(millis);
    
    try
    {
      return javax.xml.datatype.DatatypeFactory.newInstance().newXMLGregorianCalendar(cal).toString();
    }
    catch (DatatypeConfigurationException e)
    {
      throw JaqlUtil.rethrow(e);
    }

//    synchronized (ISO8601UTC_FORMAT) // TODO: write our own thread-safe formatter
//    {
//      return ISO8601UTC_FORMAT.format(millis);
//    }
  }

  /* @see com.ibm.jaql.json.type.JsonValue#getCopy(com.ibm.jaql.json.type.JsonValue) */
  @Override
  public JsonDate getCopy(JsonValue target) throws Exception
  {
    return this;  // immutable, no copying needed
  }

  @Override
  public JsonDate getImmutableCopy() throws Exception
  {
    return this;
  }

  // -- setters -----------------------------------------------------------------------------------

  /** Sets the date represented by this <code>JsonDate</code> object to the specified value.
   * 
   * @param millis the number of milliseconds since January 1, 1970, 00:00:00 GMT  
   */
  protected void set(long millis)
  {
    this.millis = millis;
  }

  /** Sets the date represented by this <code>JsonDate</code> object to the specified value.
   * 
   * @param date string representation of the date
   * @param format formatter to parse the date
   * 
   * @throws UndeclaredThrowableException when a parse error occurs
   */
  protected void set(String date, DateFormat format)
  {
    try
    {
      synchronized (format) // TODO: write our own parser code that is thread safe? 
      {
        // FIXME: add timezone support
        this.millis = format.parse(date).getTime();
      }
    }
    catch (java.text.ParseException ex)
    {
      throw new java.lang.reflect.UndeclaredThrowableException(ex);
    }
  }
  
  /** Sets the date represented by this <code>JsonDate</code> object to the specified value.
   * 
   * @param date string representation of the data in iso8601 (only UTC specified by a Z right now) 
   */
  protected void set(String date)
  {
    XMLGregorianCalendar xcal = CAL_FACTORY.newXMLGregorianCalendar(date);
    this.millis = xcal.toGregorianCalendar(UTC,Locale.getDefault(),null).getTime().getTime();
    // set(date, ISO8601UTC_FORMAT);
  }

  
  // -- comparison/hashing ------------------------------------------------------------------------
  
  /* @see com.ibm.jaql.json.type.JsonValue#compareTo(java.lang.Object) */
  @Override
  public int compareTo(Object x)
  {
    long m2 = ((JsonDate) x).millis;
    return (millis == m2) ? 0 : (millis < m2 ? -1 : +1);
  }

  /* @see com.ibm.jaql.json.type.JsonValue#longHashCode() */
  @Override
  public long longHashCode()
  {
    return JsonLong.longHashCode(millis);
  }


  // -- misc --------------------------------------------------------------------------------------
  
  /** Converts the given format string into a {@link DateFormat}. */
  public static DateFormat getFormat(String formatString)
  {
    SimpleDateFormat format = new SimpleDateFormat(formatString); // TODO: add cache of formats
    if (formatString.endsWith("'Z'") || formatString.endsWith("'z'"))
    {
      TimeZone tz = new SimpleTimeZone(0, "UTC");
      format.setTimeZone(tz);
    }
    return format;
  }
  
  /** @see com.ibm.jaql.json.type.JsonValue#getEncoding() */
  @Override
  public JsonEncoding getEncoding()
  {
    return JsonEncoding.DATE;
  }

}

