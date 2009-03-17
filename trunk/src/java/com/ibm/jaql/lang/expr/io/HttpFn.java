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
package com.ibm.jaql.lang.expr.io;

import com.ibm.jaql.io.Adapter;
import com.ibm.jaql.io.stream.StreamInputAdapter;
import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.json.type.JString;
import com.ibm.jaql.json.type.MemoryJRecord;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.JaqlFn;

/**
 * An expression that constructs an I/O descriptor for local file access.
 */
@JaqlFn(fnName="http", minArgs=1, maxArgs=3)
public class HttpFn extends AbstractHandleFn
{
  private final static Item TYPE = new Item(new JString("uri"));
  
  /**
   * exprs[0]: URL domain and path
   * exprs[1]: query
   * exprs[2]: input options
   * 
   * @param exprs
   */
  public HttpFn(Expr[] exprs)
  {
    super(exprs);
  }

  /* (non-Javadoc)
   * @see com.ibm.jaql.lang.expr.io.AbstractHandleFn#getType()
   */
  @Override
  protected Item getType()
  {
    return TYPE;
  }

  /* (non-Javadoc)
   * @see com.ibm.jaql.lang.expr.io.AbstractHandleFn#isMapReducible()
   */
  @Override
  public boolean isMapReducible()
  {
    return false;
  }
  
  /* (non-Javadoc)
   * @see com.ibm.jaql.lang.expr.io.AbstractHandleFn#eval(com.ibm.jaql.lang.core.Context)
   */
  @Override
  public Item eval(Context context) throws Exception
  {
    MemoryJRecord rec = new MemoryJRecord();
    rec.add(Adapter.TYPE_NAME, getType());
    rec.add(Adapter.LOCATION_NAME, location().eval(context));
    
    if(exprs.length > 1) {
      MemoryJRecord opts = null;
      if(exprs.length > 2) 
        opts = (MemoryJRecord)exprs[2].eval(context).get();
      else
        opts = new MemoryJRecord();
      opts.add(StreamInputAdapter.ARGS_NAME, exprs[1].eval(context));
      rec.add(Adapter.INOPTIONS_NAME, opts);
    }
    
    return new Item(rec);
  }
}
