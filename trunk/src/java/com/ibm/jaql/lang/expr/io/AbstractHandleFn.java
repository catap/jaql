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
package com.ibm.jaql.lang.expr.io;

import com.ibm.jaql.io.Adapter;
import com.ibm.jaql.json.type.BufferedJsonRecord;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;


/**
 * An expression that constructs an I/O descriptor. See {@link com.ibm.jaql.io.Adapter} for a list
 * of record fields that are specified. 
 */
public abstract class AbstractHandleFn extends Expr implements PotentialMapReducible {

  
  /**
   * @param exprs
   */
  public AbstractHandleFn(Expr[] exprs)
  {
    super(exprs);
  }
  
  /* (non-Javadoc)
   * @see com.ibm.jaql.lang.expr.io.PotentialMapReducible#isMapReducible()
   */
  public abstract boolean isMapReducible();
  
  /**
   * Return the "type" for this descriptor
   *  
   * @return
   */
  protected abstract JsonValue getType();

  /**
   * Return the "location" for this descriptor
   * 
   * @return
   */
  public Expr location() {
    return exprs[0];
  }

  /* (non-Javadoc)
   * @see com.ibm.jaql.lang.expr.core.Expr#eval(com.ibm.jaql.lang.core.Context)
   */
  @Override
  public JsonRecord eval(Context context) throws Exception
  {
    BufferedJsonRecord rec = new BufferedJsonRecord();
    rec.add(Adapter.TYPE_NAME, getType());
    rec.add(Adapter.LOCATION_NAME, location().eval(context));
    if(exprs.length > 1) {
      
      if(exprs.length == 2) {
        // only one option is specified
        rec.add(Adapter.OPTIONS_NAME, exprs[1].eval(context));
      } else if(exprs.length == 3) {
        // both input and output options are specified
        rec.add(Adapter.INOPTIONS_NAME, exprs[1].eval(context));
        rec.add(Adapter.OUTOPTIONS_NAME, exprs[2].eval(context));
      }
    }
    return rec;
  }
  
}