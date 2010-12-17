/*
 * Copyright (C) IBM Corp. 2009.
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
package com.ibm.jaql.lang.expr.date;

import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.TimeZone;

import com.ibm.jaql.json.type.BufferedJsonRecord;
import com.ibm.jaql.json.type.JsonDate;
import com.ibm.jaql.json.type.JsonEnum;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonUtil;
import com.ibm.jaql.json.type.MutableJsonLong;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;

/**
 * 
 * @jaqlDescription Return a record which stores all readable fields of a date, including year, montch, day, dayofweek ... e.g. 
 * 
 * Usage:
 * 
 * record dateParts(date d)
 * 
 * @jaqlExample dateParts(date('2000-01-01T12:00:00Z'));
 * {
 * "day": 1,
 * "dayOfWeek": 6,
 * "hour": 12,
 * "millis": 946728000000,
 * "minute": 0,
 * "month": 1,
 * "second": 0,
 * "year": 2000,
 * "zoneOffset": 0
 * }
 * 
 */
public class DatePartsFn extends Expr
{
  public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par11
  {
    public Descriptor()
    {
      super("dateParts", DatePartsFn.class);
    }
  }
  
  public static enum DatePartField implements JsonEnum
  {
    MILLIS("millis"),
    YEAR("year"),
    MONTH("month"),
    DAY("day"),
    HOUR("hour"),
    MINUTE("minute"),
    SECOND("second"),
    ZONE_OFFSET("zoneOffset"),
    DAY_OF_WEEK("dayOfWeek");
    
    public static final JsonString[] names =
      JsonUtil.jsonStrings(DatePartField.values());
    
    protected final JsonString name;
    
    private DatePartField(String name) 
    {
      this.name = new JsonString(name);
    }

    @Override
    public JsonString jsonString() 
    {
      return name; 
    }
  }

  protected GregorianCalendar cal;
  protected BufferedJsonRecord rec;
  protected MutableJsonLong[] values;
  
  public DatePartsFn(Expr[] exprs)
  {
    super(exprs);
  }
  
  protected void init()
  {
    // TODO: add timezone to JsonDate, add optional param for output zone
    cal = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
    values = new MutableJsonLong[] {
        new MutableJsonLong(), // millis
        new MutableJsonLong(), // year
        new MutableJsonLong(), // month
        new MutableJsonLong(), // day
        new MutableJsonLong(), // hour
        new MutableJsonLong(), // minute
        new MutableJsonLong(), // second
        new MutableJsonLong(), // timezone offset
        new MutableJsonLong()  // day of week 
    };
    rec = new BufferedJsonRecord(values.length);
    rec.set(DatePartField.names, values, values.length, false);
  }

  @Override
  protected JsonRecord evalRaw(Context context) throws Exception
  {
    JsonDate d = (JsonDate)exprs[0].eval(context);
    if( d == null )
    {
      return null;
    }
    if( cal == null )
    {
      init();
    }
    long millis = d.get();
    cal.setTimeInMillis(millis);
    // TODO: add timezone to JsonDate, add optional param for output zone
    values[DatePartField.MILLIS.ordinal()].set(millis);
    values[DatePartField.YEAR.ordinal()  ].set(cal.get(Calendar.YEAR));
    values[DatePartField.MONTH.ordinal() ].set(cal.get(Calendar.MONTH)+1);
    values[DatePartField.DAY.ordinal()   ].set(cal.get(Calendar.DAY_OF_MONTH));
    values[DatePartField.HOUR.ordinal()  ].set(cal.get(Calendar.HOUR_OF_DAY));
    values[DatePartField.MINUTE.ordinal()].set(cal.get(Calendar.MINUTE));
    values[DatePartField.SECOND.ordinal()].set(cal.get(Calendar.SECOND));
    values[DatePartField.ZONE_OFFSET.ordinal()].set(cal.get(Calendar.ZONE_OFFSET) / 1000);
    values[DatePartField.DAY_OF_WEEK.ordinal()].set(cal.get(Calendar.DAY_OF_WEEK) - 1); // Sunday=0, Saturday=6
    
    return rec;
  }

}
