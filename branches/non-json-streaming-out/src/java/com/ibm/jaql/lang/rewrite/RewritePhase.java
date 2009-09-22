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
   * @param expr
   */
  protected void validateTree(Expr expr)
  {
    for (Expr e : expr.children())
    {
      if (e != null) // TODO: get those nulls out
      {
        if (e.parent() != expr)
        {
          throw new RuntimeException("invalid tree");
        }
        validateTree(e);
      }
    }
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
//            if (false)
//            {
//              System.err.println("trying rewrite: "
//                  + r.getClass().getSimpleName());
//            }
            if (r.rewrite(expr))
            {
              if (engine.traceFire)
              {
                System.err.println("fired " + r.getClass().getSimpleName());
                if (engine.explainFire)
                {
                  System.err.println();
                  start.decompile(System.err, new HashSet<Var>());
                  System.err.println();
                  System.err.println();
                }
                System.err.flush();
              }
              if (true)
              {
                validateTree(start);
              }
              if (++fireCount >= maxFire)
              {
                return;
              }
              walker.reset();
              continue walking;
            }
          }
        }
      }
    }
  }
}
