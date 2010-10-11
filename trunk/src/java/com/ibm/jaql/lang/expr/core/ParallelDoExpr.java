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

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.ibm.jaql.job.JaqlStage;
import com.ibm.jaql.job.JobGraph;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.Var;

/**
 * Run a list of pipes, perserving order as required.  Return the last pipe.
 *
 */
public class ParallelDoExpr extends DoExpr
{

  public ParallelDoExpr(Expr[] exprs)
  {
    super(exprs);
  }

  public ParallelDoExpr(Expr expr0)
  {
    super(expr0);
  }

  public ParallelDoExpr(Expr expr0, Expr expr1)
  {
    super(expr0, expr1);
  }

  public ParallelDoExpr(Expr expr0, Expr expr1, Expr expr2)
  {
    super(expr0, expr1, expr2);
  }

  public ParallelDoExpr(ArrayList<? extends Expr> exprs)
  {
    super(exprs);
  }

  public void runNonReturned(Context context) throws Exception
  {
    // TODO: most of this should be in a compile stage before eval
    JobGraph g = new JobGraph();    
    HashMap<Var,JaqlStage> outMap = new HashMap<Var, JaqlStage>();
    ByteArrayOutputStream outStream = new ByteArrayOutputStream();
    PrintStream exprText = new PrintStream(outStream);
    HashSet<Var> capturedVars = new HashSet<Var>();
    int n = exprs.length - 1;
    for(int i = 0 ; i < n ; i++)
    {
      JaqlStage s = new JaqlStage(g, context, exprs[i]);
      outStream.reset();
      capturedVars.clear();
      exprs[i].decompile(exprText, capturedVars); // TODO: separate captures from decompile
      for( Var v: capturedVars )
      {
        JaqlStage r = outMap.get(v);
        if( r != null ) // if r is null, then v better be defined outside of this block
        {
          r.before(s);
        }
      }
      if( exprs[i] instanceof BindingExpr )
      {
        BindingExpr b = (BindingExpr)exprs[i];
        outMap.put(b.var, s);
      }
    }
    //Executor executor = new Executor() { public void execute(Runnable r) { r.run(); } };
    //ExecutorService executor = Executors.newFixedThreadPool(10);
    ExecutorService executor = Executors.newCachedThreadPool();
    try
    {
      g.run(executor);
    }
    finally
    {
      executor.shutdown();
    }
  }

  @Override
  public JsonValue eval(Context context) throws Exception
  {
    runNonReturned(context);
    return exprs[exprs.length-1].eval(context);
    
  }
  
  @Override
  public JsonIterator iter(Context context) throws Exception
  {
    runNonReturned(context);
    return exprs[exprs.length-1].iter(context);
  }

}
