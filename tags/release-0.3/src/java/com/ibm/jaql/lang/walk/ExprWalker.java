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
// TODO: restore e.{pat}, e.[{pat}] ?
// TODO: add { e.{pat}, ... } ?
// TODO: should { e.pat, x: e } be a remap of x (ie, exclude x from pat) ?
/*
 *
 * ConstFunction blocks rewrites on its own body (even itself)
 *  walk const fn bodies
 *  defer and run bottom up
 *  inline regardless of number uses?
 *  without this rewrite, const recs dont inline
 *
 * Split rec constructor into value expr vars
 * const field access looks for var as value expr and uses that
 *   let $r = { x:e1, y:e2 } return ... $r.x
 *   ==> 
 *   let $x = e1, $r = { x:$x, y:e2 } return ... $x
 *
 *   let $x = e1, $r = { x:$x, y:e2 } return ... $r.x
 *   ==> 
 *   let $x = e1, $r = { x:$x, y:e2 } return ... $x
 *
 * LetInline can increase computation
 * eg, let $i = 5+3 return for $j in 1 to 4 return $i;
 *     fired LetInline
 * Add let-lifting to eval only once?
 *
 */
package com.ibm.jaql.lang.walk;

import com.ibm.jaql.lang.expr.core.Expr;

/**
 * 
 */
public abstract class ExprWalker
{
  /**
   * 
   */
  public abstract void reset();
  /**
   * @param start
   */
  public abstract void reset(Expr start);
  /**
   * @return
   */
  public abstract Expr next();
}
