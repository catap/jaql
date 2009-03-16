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
package com.ibm.jaql.lang.expr.core;

import java.io.PrintStream;
import java.util.HashSet;

import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.json.type.JRecord;
import com.ibm.jaql.json.type.JString;
import com.ibm.jaql.json.type.MemoryJRecord;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.core.VarMap;
import com.ibm.jaql.util.Bool3;

/**
 * 
 */
public class ProjPattern extends FieldExpr
{

  boolean wildcard;

  /**
   * rec = exprs[0] name = exprs[1]
   * 
   * rec.name rec.name* rec.* --------- --------- ------ nameExpr name name
   * exprs.length = 1 wildcard false true true
   * 
   * @param wildcard
   * @param exprs
   */
  public ProjPattern(boolean wildcard, Expr[] exprs)
  {
    super(exprs);
    this.wildcard = wildcard;
  }

  /**
   * @param recExpr
   * @param nameExpr
   * @param wildcard
   */
  public ProjPattern(Expr recExpr, Expr nameExpr, boolean wildcard)
  {
    this(wildcard, (nameExpr == null) ? new Expr[]{recExpr} : new Expr[]{
        recExpr, nameExpr});
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#clone(com.ibm.jaql.lang.core.VarMap)
   */
  public ProjPattern clone(VarMap varMap)
  {
    return new ProjPattern(wildcard, cloneChildren(varMap));
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.FieldExpr#staticNameMatches(com.ibm.jaql.json.type.JString)
   */
  public Bool3 staticNameMatches(JString name)
  {
    if (exprs[1] instanceof ConstExpr)
    {
      ConstExpr c = (ConstExpr) exprs[1];
      Item value = c.value;
      JString text = (JString) value.get();
      if (text == null)
      {
        return Bool3.FALSE;
      }
      if (wildcard)
      {
        if (name.startsWith(text))
        {
          return Bool3.UNKNOWN;
        }
      }
      else
      {
        if (text.equals(name))
        {
          return Bool3.TRUE;
        }
      }
      return Bool3.FALSE;
    }
    return Bool3.UNKNOWN;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.FieldExpr#decompile(java.io.PrintStream,
   *      java.util.HashSet)
   */
  public void decompile(PrintStream exprText, HashSet<Var> capturedVars)
      throws Exception
  {
    exprText.print("(");
    exprs[0].decompile(exprText, capturedVars);
    exprText.print(").");
    if (exprs.length == 2)
    {
      exprText.print("(");
      exprs[1].decompile(exprText, capturedVars);
      exprText.print(")");
    }
    if (wildcard)
    {
      exprText.print("*");
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.FieldExpr#eval(com.ibm.jaql.lang.core.Context,
   *      com.ibm.jaql.json.type.MemoryJRecord)
   */
  public void eval(Context context, MemoryJRecord outrec) throws Exception
  {
    JRecord inrec = (JRecord) exprs[0].eval(context).get();
    if (inrec != null)
    {
      if (exprs.length == 2) // rec.name or rec.name*
      {
        JString name = (JString) exprs[1].eval(context).get();

        if (!wildcard) // rec.name
        {
          Item value = inrec.getValue(name, null);
          if (value != null)
          {
            outrec.add(name, value);
          }
        }
        else
        // rec.name*
        {
          int m = inrec.arity();
          for (int j = 0; j < m; j++)
          {
            JString inName = inrec.getName(j);
            if (inName.startsWith(name))
            {
              outrec.add(inName, inrec.getValue(j));
            }
          }
        }
      }
      else
      // rec.*
      {
        assert wildcard;
        int m = inrec.arity();
        for (int j = 0; j < m; j++)
        {
          outrec.add(inrec.getName(j), inrec.getValue(j));
        }
      }
    }
  }
}
