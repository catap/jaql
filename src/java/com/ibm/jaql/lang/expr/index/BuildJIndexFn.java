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

import com.ibm.jaql.io.index.JIndexWriter;
import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.json.type.JArray;
import com.ibm.jaql.json.type.JRecord;
import com.ibm.jaql.json.type.JString;
import com.ibm.jaql.json.util.Iter;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.JaqlFn;


@JaqlFn(fnName = "buildJIndex", minArgs = 2, maxArgs = 2)
public class BuildJIndexFn extends Expr
{
  public BuildJIndexFn(Expr[] exprs)
  {
    super(exprs);
  }

  @Override
  public Item eval(Context context) throws Exception
  {
    Item fdItem = exprs[1].eval(context);
    JRecord fd = (JRecord)fdItem.get();
    if( fd == null )
    {
      return Item.nil;
    }
    JString jloc = (JString)fd.getRequired("location").get();
    if( jloc == null )
    {
      return Item.nil;
    }

    JIndexWriter index = new JIndexWriter(jloc.toString());

    Item[] kvpair = new Item[2];

    Item item;
    Iter iter = exprs[0].iter(context);
    while( (item = iter.next()) != null )
    {
      JArray arr = (JArray)item.get();
      arr.getTuple(kvpair);
      Item key = kvpair[0];
      Item val = kvpair[1];
      index.add(key,val);
    }
    
    index.close();
    
    return fdItem;
  }
}
