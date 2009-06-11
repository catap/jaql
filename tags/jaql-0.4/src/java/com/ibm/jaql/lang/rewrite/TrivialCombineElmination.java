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

import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.lang.expr.core.ArrayExpr;
import com.ibm.jaql.lang.expr.core.BindingExpr;
import com.ibm.jaql.lang.expr.core.CombineExpr;
import com.ibm.jaql.lang.expr.core.ConstExpr;
import com.ibm.jaql.lang.expr.core.Expr;

/**
 * combine $a, $b in [e1] using e2($a,$b) ==> e1
 * 
 * combine $a, $b in [] using e2($a,$b) ==> null
 * 
 * combine $a, $b in null using e2($a,$b) ==> null
 * 
 * Decided against on empty... combine $a, $b in [e1] using e2($a,$b) on empty
 * e3 ==> firstNonNull(e1,e3)
 * 
 * combine $a, $b in [] using e2($a,$b) on empty e3 ==> e3
 * 
 * combine $a, $b in null using e2($a,$b) on empty e3 ==> null
 * 
 * TODO: add combine-null elmination (a la denull) combine $a, $b in [..., null,
 * ...] return e2($a,$b) ==> combine $a, $b in [..., ...] return e2($a,$b)
 * 
 */
public class TrivialCombineElmination extends Rewrite
{
  /**
   * @param phase
   */
  public TrivialCombineElmination(RewritePhase phase)
  {
    super(phase, CombineExpr.class);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.rewrite.Rewrite#rewrite(com.ibm.jaql.lang.expr.core.Expr)
   */
  @Override
  public boolean rewrite(Expr expr)
  {
    CombineExpr ce = (CombineExpr) expr;

    if( true ) return false; // TODO: re-enable with function api?

    BindingExpr bind = null;// ce.binding();
    Expr inExpr = bind.inExpr();

    // combine $a,$b in null ... => null
    if (inExpr instanceof ConstExpr && ((ConstExpr) inExpr).value.get() == null)
    {
      ce.replaceInParent(inExpr);
      return true;
    }

    if (!(inExpr instanceof ArrayExpr) || inExpr.numChildren() > 1)
    {
      return false;
    }

    Expr replaceBy;
    if (inExpr.numChildren() == 0)
    {
      // combine $a,$b in [] ... => null
      replaceBy = new ConstExpr(Item.NIL);
      //      replaceBy = ce.emptyExpr();
    }
    else
    // arrayExpr.numChildren() == 1
    {
      replaceBy = inExpr.child(0);
      //      if( replaceBy.isNullable() )
      //      {
      //        replaceBy = new FirstNonNullFn(replaceBy, ce.emptyExpr());
      //      }
    }
    expr.replaceInParent(replaceBy);
    return true;
  }
}
