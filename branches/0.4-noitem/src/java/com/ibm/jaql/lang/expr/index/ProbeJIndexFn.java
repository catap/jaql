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
package com.ibm.jaql.lang.expr.index;

import com.ibm.jaql.io.index.JIndexReader;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.IterExpr;
import com.ibm.jaql.lang.expr.core.JaqlFn;


@JaqlFn(fnName = "probeJIndex", minArgs = 2, maxArgs = 2)
public class ProbeJIndexFn extends IterExpr
{
  protected JIndexReader index;
  
  public ProbeJIndexFn(Expr[] exprs)
  {
    super(exprs);
  }

  @Override
  public JsonIterator iter(Context context) throws Exception
  {
    if( index == null )
    {
      JsonRecord fd = (JsonRecord)exprs[0].eval(context);
      if( fd == null )
      {
        return JsonIterator.NIL;
      }
      JsonString jloc = (JsonString)fd.getValue("location");
      if( jloc == null )
      {
        return JsonIterator.NIL;
      }
      index = new JIndexReader(jloc.toString());
      context.closeAtQueryEnd(index);
    }
    
    JsonRecord jrange = (JsonRecord)exprs[1].eval(context);
    JsonValue low = jrange.getValue("low", null);
    JsonValue high = jrange.getValue("high", null);
    return index.rangeScan(low, high);
  }
}
