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

import java.util.HashSet;
import java.util.Map;

import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.type.JsonBool;
import com.ibm.jaql.json.type.JsonType;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.core.VarMap;
import com.ibm.jaql.util.FastPrinter;

/**
 * 
 */
public class CompareExpr extends Expr
{
  public static final int      EQ = 0;
  public static final int      NE = 1;
  public static final int      LT = 2;
  public static final int      LE = 3;
  public static final int      GT = 4;
  public static final int      GE = 5;

  public static final String[] OP = {"==", "!=", "<", "<=", ">", ">="};

  int                          op;

  /**
   * @param op
   * @param exprs
   */
  public CompareExpr(int op, Expr[] exprs)
  {
    super(exprs);
    this.op = op;
  }

  /**
   * @param op
   * @param expr1
   * @param expr2
   */
  public CompareExpr(int op, Expr expr1, Expr expr2)
  {
    this(op, new Expr[]{expr1, expr2});
  }

  @Override
  public Map<ExprProperty, Boolean> getProperties() 
  {
    Map<ExprProperty, Boolean> result = super.getProperties();
    result.put(ExprProperty.ALLOW_COMPILE_TIME_COMPUTATION, true);
    return result;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#clone(com.ibm.jaql.lang.core.VarMap)
   */
  public Expr clone(VarMap varMap)
  {
    return cloneOrigin(new CompareExpr(op, cloneChildren(varMap)));
  }
  
  @Override
  public Schema getSchema()
  {
    return SchemaFactory.booleanOrNullSchema();
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#decompile(java.io.PrintStream,
   *      java.util.HashSet)
   */
  protected void decompileRaw(FastPrinter exprText, HashSet<Var> capturedVars, boolean emitLocation)
      throws Exception
  {
    exprText.print("(");
    exprs[0].decompile(exprText, capturedVars,emitLocation);
    exprText.print(") ");
    exprText.print(OP[op]);
    exprText.print(" (");
    exprs[1].decompile(exprText, capturedVars,emitLocation);
    exprText.print(")");
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#eval(com.ibm.jaql.lang.core.Context)
   */
  protected JsonValue evalRaw(final Context context) throws Exception
  {
    JsonValue value1 = exprs[0].eval(context);
    if (value1 == null)
    {
      return null;
    }
    JsonValue value2 = exprs[1].eval(context);
    if (value2 == null)
    {
      return null;
    }
    
    // check types (different types cannot be compared in general, numeric types are exception)
    if (JsonType.typeCompare(value1, value2) != 0 
        && !(value1.getType().isNumber() && value2.getType().isNumber()))
    {
      return null;
    }

    // FIXME: arrays are reporting true/false when the should return null, eg: ['5'] < [5];
    boolean b;
    int c = value1.compareTo(value2);

    if (value1.getType() == JsonType.RECORD)
    {
      // FIXME: record inside arrays are still compared, also need to fix sort
      b = (c == 0);

      switch (op)
      {
        case EQ :
          break;
        case NE :
          b = !b;
          break;
        case LT :
        case GT :
          // not( null or a = b ) => a=b ? false : null;
          if (!b)
          {
            return null;
          }
          b = false;
          break;
        case LE :
        case GE :
          // (null or a = b) => a=b ? true : null;
          if (!b)
          {
            return null;
          }
          break;
        default :
          throw new RuntimeException("should not get here!");
      }
    }
    else
    {
      switch (op)
      {
        case EQ :
          b = (c == 0);
          break;
        case NE :
          b = (c != 0);
          break;
        case LT :
          b = (c < 0);
          break;
        case LE :
          b = (c <= 0);
          break;
        case GT :
          b = (c > 0);
          break;
        case GE :
          b = (c >= 0);
          break;
        default :
          throw new RuntimeException("should not get here!");
      }
    }
    return JsonBool.make(b);
  }
}
