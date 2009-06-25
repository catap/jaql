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
import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.json.type.JRecord;
import com.ibm.jaql.json.type.JString;
import com.ibm.jaql.json.util.Iter;
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
  public Iter iter(Context context) throws Exception
  {
    if( index == null )
    {
      Item fdItem = exprs[0].eval(context);
      JRecord fd = (JRecord)fdItem.get();
      if( fd == null )
      {
        return Iter.nil;
      }
      JString jloc = (JString)fd.getValue("location").get();
      if( jloc == null )
      {
        return Iter.nil;
      }
      index = new JIndexReader(jloc.toString());
      context.closeAtQueryEnd(index);
    }
    
    Item range = exprs[1].eval(context);
    JRecord jrange = (JRecord)range.get();
    Item low = jrange.getValue("low", null);
    Item high = jrange.getValue("high", null);
    return index.rangeScan(low, high);
  }
}
