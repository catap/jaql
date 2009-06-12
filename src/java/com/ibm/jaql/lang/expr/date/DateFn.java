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

import java.text.SimpleDateFormat;
import java.util.Date;

import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.json.type.JDate;
import com.ibm.jaql.json.type.JString;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.JaqlFn;

@JaqlFn(fnName="date", minArgs=1, maxArgs=2)
public class DateFn extends Expr
{
  public DateFn(Expr[] exprs)
  {
    super(exprs);
  }

  @Override
  public Item eval(Context context) throws Exception
  {
    String f = JDate.iso8601UTCFormat;
    if( exprs.length == 2 )
    {
      JString js = (JString)exprs[1].eval(context).get();
      if( js != null )
      {
        f = js.toString();
      }
    }
    SimpleDateFormat format = new SimpleDateFormat(f); // TODO: memory
    
    Item s = exprs[0].eval(context);
    // TODO: support date(millis)
    JString js = (JString)s.get();
    
    Date date = format.parse(js.toString());
    long millis = date.getTime();
    millis -= format.getTimeZone().getRawOffset();
    JDate d = new JDate(millis); // TODO: memory
    Item item = new Item(d); // TODO: memory
    return item;
  }

}
