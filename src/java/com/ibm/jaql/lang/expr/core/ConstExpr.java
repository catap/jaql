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
import com.ibm.jaql.json.type.JArray;
import com.ibm.jaql.json.type.JDate;
import com.ibm.jaql.json.type.JLong;
import com.ibm.jaql.json.type.JRecord;
import com.ibm.jaql.json.type.JRegex;
import com.ibm.jaql.json.type.JString;
import com.ibm.jaql.json.type.JValue;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.JFunction;
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.core.VarMap;
import com.ibm.jaql.util.Bool3;

/**
 * 
 */
public final class ConstExpr extends Expr
{
  public Item value;

  /**
   * @param value
   */
  public ConstExpr(Item value)
  {
    super(NO_EXPRS);
    this.value = value;
  }

  /**
   * @param value
   */
  public ConstExpr(JValue value)
  {
    this(new Item(value));
  }

  /**
   * 
   * @param value
   */
  public ConstExpr(String value)
  {
    this(new Item(new JString(value)));
  }

  public ConstExpr(long v)
  {
    this(new Item(new JLong(v)));
  }
  
  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#isNull()
   */
  @Override
  public Bool3 isNull()
  {
    return Bool3.valueOf(value.get() == null);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#isArray()
   */
  @Override
  public Bool3 isArray()
  {
    JValue v = value.get();
    return Bool3.valueOf(v == null || v instanceof JArray);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#decompile(java.io.PrintStream,
   *      java.util.HashSet)
   */
  public void decompile(PrintStream exprText, HashSet<Var> capturedVars)
      throws Exception
  {
    JValue w = value.get();
    boolean annotate = 
      w instanceof JArray    || 
      w instanceof JRecord   || 
      w instanceof JFunction || // TODO: JValue.getType().isExtendedJson()
      w instanceof JDate     || // TODO: parser should recognize constructors and eval during parse
      w instanceof JRegex;
    if (annotate) exprText.print("const("); // FIXME: remove
    value.print(exprText, 2);
    if (annotate) exprText.print(")");// FIXME: remove
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#eval(com.ibm.jaql.lang.core.Context)
   */
  public Item eval(Context context) throws Exception
  {
    return value;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#clone(com.ibm.jaql.lang.core.VarMap)
   */
  public ConstExpr clone(VarMap varMap)
  {
    return new ConstExpr(value);
  }
}
