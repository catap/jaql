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

import com.ibm.jaql.io.registry.RegistryUtil;
import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.json.type.JString;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.JaqlFn;
import com.ibm.jaql.lang.util.JaqlUtil;

/**
 * Load the registry from the given file
 * 
 * readAdapterRegistry(string filename) returns file name
 */
@JaqlFn(fnName = "readAdapterRegistry", minArgs = 1, maxArgs = 1)
public class ReadAdapterRegistryExpr extends Expr
{
  public ReadAdapterRegistryExpr(Expr[] exprs)
  {
    super(exprs);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#eval(com.ibm.jaql.lang.core.Context)
   */
  @Override
  public Item eval(Context context) throws Exception
  {
    Item fileItem = exprs[0].eval(context);
    JString fileName = (JString) fileItem.getNonNull();

    RegistryUtil.readFile(fileName.toString(), JaqlUtil.getAdapterStore());
    return fileItem;
  }
}
