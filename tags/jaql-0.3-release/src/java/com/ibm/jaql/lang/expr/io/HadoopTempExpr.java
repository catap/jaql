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
import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.json.type.JString;
import com.ibm.jaql.json.type.MemoryJRecord;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.JaqlFn;

/**
 * 
 */
@JaqlFn(fnName = "HadoopTemp", minArgs = 0, maxArgs = 0)
public class HadoopTempExpr extends Expr
{
  /**
   * @param exprs
   */
  public HadoopTempExpr(Expr[] exprs)
  {
    super(exprs);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#isConst()
   */
  @Override
  public boolean isConst()
  {
    return false;
  }

  /**
   * 
   */
  public HadoopTempExpr()
  {
    super(NO_EXPRS);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.Expr#eval(com.ibm.jaql.lang.Context)
   */
  public Item eval(Context context) throws Exception
  {
    // FIXME: figure out where this should go
    //String filename = "temp/jaql_temp_" + System.nanoTime(); // FIXME: this works for me in standalone mode.
    //String filename = "jaql_temp_" + System.nanoTime(); // FIXME: this works in the cluster...
    // FIXME: need to delete temps!!
    String filename = "jaql_temp_" + System.nanoTime();
    // String filename = "c:/temp/jaql_temp_" + System.nanoTime();
    // String filename = "/temp/jaql_temp_" + System.nanoTime();
    MemoryJRecord r = new MemoryJRecord();
    r.add(Adapter.TYPE_NAME, new Item(new JString("hdfs")));
    r.add(Adapter.LOCATION_NAME, new Item(new JString(filename)));
    return new Item(r); // TODO: memory
  }
}
