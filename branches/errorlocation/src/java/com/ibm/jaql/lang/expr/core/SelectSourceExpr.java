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
package com.ibm.jaql.lang.expr.core;
import java.util.ArrayList;

import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.util.JaqlUtil;


public class SelectSourceExpr extends IterExpr
{

  public SelectSourceExpr(Expr[] inputs)
  {
    super(inputs);
  }
  
  public SelectSourceExpr(ArrayList<Expr> inputs)
  {
    super(inputs);
  }

  @Override
  protected JsonIterator iterRaw(final Context context) throws Exception
  {
    int n = exprs.length - 1;
    for(int i = 0 ; i < n ; i++)
    {
      IfExpr ifExpr = (IfExpr)exprs[i]; // TODO: using IfExpr is a hack; introduce new pseudo expr
      if( JaqlUtil.ebv(ifExpr.testExpr().eval(context)) )
      {
        return ifExpr.trueExpr().iter(context);
      }
    }
    return JsonIterator.EMPTY; // exprs[n].iter(context);
  }

}
