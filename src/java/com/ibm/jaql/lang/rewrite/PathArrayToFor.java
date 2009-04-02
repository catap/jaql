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
package com.ibm.jaql.lang.rewrite;

import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.expr.array.ToArrayFn;
import com.ibm.jaql.lang.expr.core.ArrayExpr;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.ForExpr;
import com.ibm.jaql.lang.expr.core.VarExpr;
import com.ibm.jaql.lang.expr.path.PathArray;
import com.ibm.jaql.lang.expr.path.PathArrayAll;
import com.ibm.jaql.lang.expr.path.PathExpand;
import com.ibm.jaql.lang.expr.path.PathExpr;
import com.ibm.jaql.lang.expr.path.PathReturn;
import com.ibm.jaql.lang.expr.path.PathStep;
import com.ibm.jaql.lang.expr.path.PathToArray;

/**
 * e [*] p ==> for( $i in         e  ) [ $i p ]
 * e [?] p ==> for( $i in toArray(e) ) [ $i p ]
 * e []  p ==> for( $i in toArray(e) ) toArray( $i p )
 */
public class PathArrayToFor extends Rewrite
{
  /**
   * @param phase
   */
  public PathArrayToFor(RewritePhase phase)
  {
    super(phase, PathArray.class);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.rewrite.Rewrite#rewrite(com.ibm.jaql.lang.expr.core.Expr)
   */
  @Override
  public boolean rewrite(Expr expr)
  {
    if( !( expr.parent() instanceof PathExpr ) )
    {
      return false;
    }
    
    if( !( expr instanceof PathArrayAll ||
           expr instanceof PathToArray ||
           expr instanceof PathExpand ) )
    {
      return false;
    }
    
    PathExpr pe = (PathExpr)expr.parent();

    Expr outer = pe.input();
    if( ! outer.isArray().always() &&
        ( expr instanceof PathToArray ||
          expr instanceof PathExpand ) )
    {
      outer = new ToArrayFn(outer);
    }

    Var v = engine.env.makeVar("$");
    PathStep nextStep = ((PathArray)expr).nextStep();
    Expr inner = new VarExpr(v);
    if( !( nextStep instanceof PathReturn ) )
    {
      inner =  new PathExpr( inner, nextStep );
    }

    if( expr instanceof PathArrayAll ||
        expr instanceof PathToArray )
    {
      inner = new ArrayExpr(inner);
    }
    else if( ! nextStep.isArray().always() )
    {
      assert expr instanceof PathExpand;
      inner = new ToArrayFn(inner);
    }
    
    ForExpr fe = new ForExpr(v, outer, inner);
    pe.replaceInParent(fe);
    return true;
  }
}
