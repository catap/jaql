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
package com.ibm.jaql.lang.core;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.ArrayList;
import java.util.HashMap;

import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.util.JaqlUtil;
import com.ibm.jaql.util.Pair;

/** Run-time context, i.e., values for the variables in the environment.
 * 
 */
public class Context
{
  // protected HashMap<Var,Object> varValues = new HashMap<Var,Object>();
  // protected HashMap<Expr,Item>  tempArrays = new HashMap<Expr,Item>(); // TODO: we could use one hashmap
  protected HashMap<Pair<Expr,JaqlFunction>,JaqlFunction> fnMap = new HashMap<Pair<Expr,JaqlFunction>,JaqlFunction>(); // TODO: this will be a compiled expr soon 
  protected Pair<Expr,JaqlFunction> exprFnPair = new Pair<Expr, JaqlFunction>();
  protected ArrayList<Runnable> resetTasks = new ArrayList<Runnable>();
  // PyModule pyModule;

  /**
   * Create a new root context.
   */
  public Context()
  {
    // TODO: come up with a really reliable way to cleanup temp files, etc.
    
    //    final Context me = this;
    //    Runtime.getRuntime().addShutdownHook(new Thread() {
    //      @Override public void run() { me.reset(); };
    //    });
//  if( JaqlUtil.getSessionContext() == null )
//  {
//    PySystemState systemState = Py.getSystemState();
//    if (systemState == null)
//    {
//      systemState = new PySystemState();
//    }
//    Py.setSystemState(systemState);
//    pyModule = new PyModule("jaqlMain", new PyStringMap());
//  }
  }
  
  // public PyModule getPyModule() { return JaqlUtil.getSessionContext().pyModule; }
  
  /** 
   * Clears the context.
   */
  public void reset()
  {
    // varValues.clear();
    // tempArrays.clear();
    fnMap.clear(); 
    exprFnPair.a = null;
    exprFnPair.b = null;
    for(Runnable task: resetTasks)
    {
      try
      {
        task.run();
      }
      catch(Throwable e)
      {
        e.printStackTrace(); // TODO: log
      }
    }
    resetTasks.clear();
    JaqlUtil.getQueryPageFile().clear();
  }


  public void doAtReset(Runnable task)
  {
    resetTasks.add(task);
  }
  
  /**
   * In case a context gets lost, we will close it when garbage collected.
   * This can happen when exceptions occur or because functions that return an Iter
   * might not be run until completion (which is when they close their context).
   */
  protected void finalize() throws Throwable 
  {
    try
    {
      reset();
    }
    finally
    {
      super.finalize();
    }
  }

  public void closeAtQueryEnd(final Closeable resource)
  {
    doAtReset(new Runnable() {
      @Override
      public void run()
      {
        try
        {
          resource.close();
        }
        catch (IOException e)
        {
          throw new UndeclaredThrowableException(e);
        }
      }
    });
  }

  public JaqlFunction getCallable(Expr callSite, JaqlFunction fn) throws Exception
  {
    exprFnPair.a = callSite;
    exprFnPair.b = fn;
    JaqlFunction fn2 = fnMap.get(exprFnPair);
    if( fn2 == null )
    {
      fn2 = new JaqlFunction();
      fn2.setCopy(fn);
      Pair<Expr,JaqlFunction> p = new Pair<Expr, JaqlFunction>(callSite, fn);
      fnMap.put(p, fn2);
    }
    return fn2;
  }

  public File createTempFile(String prefix, String suffix) throws IOException
  {
    final File f = File.createTempFile(prefix, suffix);
    f.deleteOnExit();
    doAtReset( new Runnable() {
      @Override 
      public void run()
      {
        f.delete();
      }
    });
    return f;
  }
}
