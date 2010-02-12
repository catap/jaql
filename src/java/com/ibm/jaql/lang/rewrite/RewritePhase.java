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
package com.ibm.jaql.lang.rewrite;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.walk.ExprWalker;

/**
 * 
 */
public class RewritePhase
{
  public RewriteEngine                                      engine;
  public ExprWalker                                         walker;
  public int                                                maxFire = 0;
  public HashMap<Class<? extends Expr>, ArrayList<Rewrite>> rules   = new HashMap<Class<? extends Expr>, ArrayList<Rewrite>>();

  /**
   * @param engine
   * @param walker
   * @param maxFire
   */
  public RewritePhase(RewriteEngine engine, ExprWalker walker, int maxFire)
  {
    this.engine = engine;
    this.walker = walker;
    this.maxFire = maxFire;
  }

  /**
   * @param r
   * @param fireOn
   */
  public void fireOn(Rewrite r, Class<? extends Expr> fireOn)
  {
    ArrayList<Rewrite> rs = rules.get(fireOn);
    if (rs == null)
    {
      rs = new ArrayList<Rewrite>();
      rules.put(fireOn, rs);
    }
    rs.add(r);
  }

  /**
   * @param r
   * @param fireOn
   */
  public void fireOn(Rewrite r, Class<? extends Expr>[] fireOn)
  {
    for (Class<? extends Expr> c : fireOn)
    {
      fireOn(r, c);
    }
  }

  /**
   * Returns true if the tree is valid.  Raises an exception otherwise.
   * @param expr
   */
  protected boolean validateTree(Expr expr)
  {
    for (Expr e : expr.children())
    {
      if (e != null) // TODO: get those nulls out
      {
        if (e.parent() != expr)
        {
          throw new IllegalStateException("Invalid Expr tree: child does not point back to parent");
        }
        validateTree(e);
      }
    }
    return true;
  }

  /**
   * @param start
   * @throws Exception
   */
  public void run(Expr start) throws Exception
  {
    if (maxFire <= 0)
    {
      return;
    }
    int fireCount = 0;
    HashMap<Class<? extends Expr>, ArrayList<Rewrite>> rules = this.rules;
    ExprWalker walker = this.walker;
    walker.reset(start);
    Expr expr;
    final boolean traceFire = engine.traceFire;
    final boolean traceTryRule = false; // engine.traceTryRule;
    final boolean tracing = traceFire || traceTryRule;
    long numTried = 0;
    long lastFired = System.nanoTime();
    walking : while ((expr = walker.next()) != null)
    {
      for (Class<?> exprClass = expr.getClass(); exprClass != Object.class; exprClass = exprClass
          .getSuperclass())
      {
        ArrayList<Rewrite> myRules = rules.get(exprClass);
        if (myRules != null)
        {
          int n = myRules.size();
          for (int i = 0; i < n; i++)
          {
            Rewrite r = myRules.get(i);
            long startTime = tracing ? System.nanoTime() : 0;
            if (r.rewrite(expr))
            {
              if (traceFire)
              {
                long now = System.nanoTime();
                if( true /*!traceTryRule*/ )
                {
                  System.err.println("{ type:'skip', numTried:"+numTried+", nanos:"+(now-lastFired)+" },");
                }
                System.err.println("{ type:'fired', rule:'" + r.getClass().getSimpleName() + "', nanos:"+(now-startTime)+" },");
                if (engine.explainFire)
                {
                  System.err.println();
//                  VarTagger.tag(start);
                  start.decompile(System.err, new HashSet<Var>());
                  System.err.println();
                  System.err.println();
                }
                System.err.flush();
                lastFired = now;
                numTried = 0;
              }
              assert validateTree(start);
              if (++fireCount >= maxFire)
              {
                return;
              }
              walker.reset();
              continue walking;
            }
            else 
            {
              numTried++;
              if( traceTryRule )
              {
                long now = System.nanoTime();
                System.err.println("{ type:'tried', rule:'" + r.getClass().getSimpleName() + "', nanos:"+(now-startTime)+" },");
              }
            }
          }
        }
      }
    }
  }
}
