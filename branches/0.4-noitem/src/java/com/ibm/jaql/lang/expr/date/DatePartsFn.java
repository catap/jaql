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

import java.math.BigDecimal;
import java.util.Calendar;
import java.util.GregorianCalendar;

import com.ibm.jaql.json.type.BufferedJsonRecord;
import com.ibm.jaql.json.type.JsonDate;
import com.ibm.jaql.json.type.JsonDecimal;
import com.ibm.jaql.json.type.JsonLong;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.JaqlFn;

@JaqlFn(fnName="dateParts", minArgs=1, maxArgs=1)
public class DatePartsFn extends Expr
{
  protected GregorianCalendar cal = new GregorianCalendar();
  
  public DatePartsFn(Expr[] exprs)
  {
    super(exprs);
  }

  @Override
  public JsonRecord eval(Context context) throws Exception
  {
    JsonDate d = (JsonDate)exprs[0].eval(context);
    if( d == null )
    {
      return null;
    }
    BufferedJsonRecord rec = new BufferedJsonRecord(); // TODO: mucho memory
    cal.setTimeInMillis(d.millis);
    rec.add("millis", new JsonLong(d.millis));
    rec.add("year", new JsonLong(cal.get(Calendar.YEAR)));
    rec.add("month", new JsonLong(cal.get(Calendar.MONTH)+1));
    rec.add("day", new JsonLong(cal.get(Calendar.DAY_OF_MONTH)));
    rec.add("hour", new JsonLong(cal.get(Calendar.HOUR_OF_DAY)));
    rec.add("minute", new JsonLong(cal.get(Calendar.MINUTE)));
    BigDecimal dec = 
      new BigDecimal( cal.get(Calendar.SECOND) * 1000 + cal.get(Calendar.MILLISECOND))
       .divide(new BigDecimal(1000));
    rec.add("second", new JsonDecimal(dec));
    rec.add("offset", new JsonLong(cal.get(Calendar.ZONE_OFFSET) / 1000));
    // TODO: add timezone to JDate
    
    return rec;
  }

}
