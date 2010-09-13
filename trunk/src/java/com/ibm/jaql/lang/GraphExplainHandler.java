/*
 * Copyright (C) IBM Corp. 2010.
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
package com.ibm.jaql.lang;

import com.ibm.jaql.json.type.JsonArray;
import com.ibm.jaql.lang.expr.core.ConstExpr;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.io.RegisterAdapterExpr;
import com.ibm.jaql.lang.expr.top.ExplainExpr;
import com.ibm.jaql.lang.expr.top.QueryExpr;

public class GraphExplainHandler extends ExplainHandler
{
  public GraphExplainHandler()
  {
    // TODO: write to file instead of returning?
  }
  
  @Override
  public Expr explain(Expr expr) throws Exception
  {
    if( expr instanceof QueryExpr && expr.child(0) instanceof RegisterAdapterExpr ) // HACK: if we don't register, explain will change or bomb. This will go away with the registry.
    {
      return expr;
    }
    ExplainExpr explain = (ExplainExpr)expr;     // HACK: gross; only needed until graph code is moved around.
    JsonArray graph = explain.buildGraph();
    return new ConstExpr(graph);
  }
}
