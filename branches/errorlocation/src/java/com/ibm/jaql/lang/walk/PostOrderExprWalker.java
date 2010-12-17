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
package com.ibm.jaql.lang.walk;

import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.util.IntArray;

/**
 * 
 */
public class PostOrderExprWalker extends ExprWalker
{
  // static boolean assertsEnabled = false;
  // static { assert assertsEnabled = true; }  // Intentional side effect!!!
  Expr     start;
  Expr     cur;
  IntArray slots = new IntArray(); // index into Expr.exprs of current path from root to cur

  /**
   * reset must be called;
   */
  public PostOrderExprWalker()
  {
  }

  /**
   * @param start
   */
  public PostOrderExprWalker(Expr start)
  {
    reset(start);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.walk.ExprWalker#reset(com.ibm.jaql.lang.expr.core.Expr)
   */
  public void reset(Expr start)
  {
    this.start = start;
    this.cur = start;
    slots.clear();
    // start at the first leaf
    if (start != null)
    {
      while (cur.numChildren() > 0)
      {
        cur = cur.child(0);
        slots.add(0);
      }
    }
  }

  /**
   * @param start
   */
  public void reset1(Expr start)
  {
    this.start = start;
    this.cur = start;
    slots.clear();
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.walk.ExprWalker#reset()
   */
  public void reset()
  {
    reset(start);
  }

  /**
   * @return
   */
  public Expr getCur()
  {
    return cur;
  }

  /**
   * Return the next in a depth-first, pre-order traversal
   * 
   * @return
   */
  public Expr next1()
  {
    if (cur == null)
    {
      return null;
    }
    if (cur.numChildren() > 0)
    {
      // Go down the tree
      slots.add(0);
      cur = cur.child(0);
      return cur;
    }
    else
    {
      // Go up until we can go across the tree
      int slot;
      do
      {
        if (slots.empty())
        {
          cur = null;
          return null;
        }
        cur = cur.parent();
        slot = slots.pop() + 1;
      } while (slot >= cur.numChildren());

      slots.add(slot);
      cur = cur.child(slot);
      return cur;
    }
  }

  /*
   * Return the next in a post-order traversal (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.walk.ExprWalker#next()
   */
  public Expr next()
  {
    Expr ret = cur;
    if (ret == start || ret == null)
    {
      cur = null;
    }
    else
    {
      assert ret.getDepth(start) == slots.size() : "bad expr walk state:"
          + ret.getDepth(start) + "!=" + slots.size();

      // pop up to the parent and go to the first descendant leaf of the next sibling, if it exists
      cur = ret.parent();
      int slot = slots.pop() + 1;
      while (slot < cur.numChildren())
      {
        Expr child = cur.child(slot);
        assert child.parent() == cur : cur.getClass() + " not parent of " + child.getClass();
        cur = child;
        slots.add(slot);
        slot = 0;
      }
    }
    return ret;
  }
}
