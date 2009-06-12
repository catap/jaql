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
import java.math.BigDecimal;
import java.math.MathContext;
import java.util.HashSet;

import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.json.type.JDate;
import com.ibm.jaql.json.type.JDecimal;
import com.ibm.jaql.json.type.JDouble;
import com.ibm.jaql.json.type.JLong;
import com.ibm.jaql.json.type.JNumber;
import com.ibm.jaql.json.type.JNumeric;
import com.ibm.jaql.json.type.JString;
import com.ibm.jaql.json.type.JValue;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.core.VarMap;

/**
 * 
 */
public class MathExpr extends Expr
{
  public static final int      PLUS     = 0;
  public static final int      MINUS    = 1;
  public static final int      MULTIPLY = 2;
  public static final int      DIVIDE   = 3;

  public static final String[] OP_STR   = {"+", "-", "*", "/"};

  protected int                op;

  /**
   * @param expr
   * @return
   */
  public static Expr negate(Expr expr)
  {
    if (expr instanceof ConstExpr)
    {
      ConstExpr ce = (ConstExpr) expr;
      JValue t = ce.value.get();
      if (t instanceof JNumeric)
      {
        ((JNumeric) t).negate();
        return expr;
      }
    }
    return new MathExpr(MathExpr.MINUS, new ConstExpr(JLong.ZERO), expr);
  }

  /**
   * @param op
   * @param expr1
   * @param expr2
   */
  public MathExpr(int op, Expr expr1, Expr expr2)
  {
    super(new Expr[]{expr1, expr2});
    this.op = op;
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
    exprText.print("(");
    exprs[0].decompile(exprText, capturedVars);
    exprText.print(")" + OP_STR[op] + "(");
    exprs[1].decompile(exprText, capturedVars);
    exprText.print(")");
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#clone(com.ibm.jaql.lang.core.VarMap)
   */
  public Expr clone(VarMap varMap)
  {
    return new MathExpr(op, exprs[0].clone(varMap), exprs[1].clone(varMap));
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#eval(com.ibm.jaql.lang.core.Context)
   */
  public Item eval(Context context) throws Exception
  {
    JValue item1 = exprs[0].eval(context).get();
    if (item1 == null)
    {
      return Item.nil;
    }
    JValue item2 = exprs[1].eval(context).get();
    if (item2 == null)
    {
      return Item.nil;
    }
    if (item1 instanceof JLong && item2 instanceof JLong)
    {
      long n1 = ((JLong) item1).value;
      long n2 = ((JLong) item2).value;
      return longEval(n1, n2);
    }
    else if (item1 instanceof JDate && item2 instanceof JDate)
    {
      long n1 = ((JDate) item1).millis;
      long n2 = ((JDate) item2).millis;
      return longEval(n1, n2);
    }
    else if (item1 instanceof JString || item2 instanceof JString)
    {
      if (op != PLUS) // TODO: use a different symbol or function for string concat? javascript uses +
      {
        throw new RuntimeException("invalid operator on strings");
      }
      // TODO: memory
      JString text1 = (JString) item1;
      JString text2 = (JString) item2;
      byte[] buf = new byte[text1.getLength() + text2.getLength()];
      System.arraycopy(text1.getBytes(), 0, buf, 0, text1.getLength());
      System.arraycopy(text2.getBytes(), 0, buf, text1.getLength(), text2
          .getLength());
      return new Item(new JString(buf));
    }
    else if (item1 instanceof JDouble || item2 instanceof JDouble)
    {
      double d1 = ((JNumeric) item1).doubleValue();
      double d2 = ((JNumeric) item2).doubleValue();
      return doubleEval(d1, d2);
    }
    else
    {
      BigDecimal n1 = ((JNumber) item1).decimalValue();
      BigDecimal n2 = ((JNumber) item2).decimalValue();
      BigDecimal n3;
      switch (op)
      {
        case PLUS : {
          n3 = n1.add(n2, MathContext.DECIMAL128);
          break;
        }
        case MINUS : {
          n3 = n1.subtract(n2, MathContext.DECIMAL128);
          break;
        }
        case MULTIPLY : {
          n3 = n1.multiply(n2, MathContext.DECIMAL128);
          break;
        }
        case DIVIDE : {
          try
          {
            n3 = n1.divide(n2, MathContext.DECIMAL128);
          }
          catch (ArithmeticException e)
          {
            // TODO: need +INF, -INF, and NaN
            return Item.nil;
          }
          break;
        }
        default :
          throw new RuntimeException("invalid op:" + op);
      }
      return new Item(new JDecimal(n3)); // TODO: reuse
    }
  }

  /**
   * @param n1
   * @param n2
   * @return
   */
  private Item longEval(long n1, long n2)
  {
    long n3;
    switch (op)
    {
      case PLUS : {
        n3 = n1 + n2;
        break;
      }
      case MINUS : {
        n3 = n1 - n2;
        break;
      }
      case MULTIPLY : {
        n3 = n1 * n2;
        break;
      }
      case DIVIDE : {
        try
        {
          // n3 = n1 / n2;
          BigDecimal d1 = new BigDecimal(n1);
          BigDecimal d2 = new BigDecimal(n2);
          BigDecimal d3 = d1.divide(d2, MathContext.DECIMAL128);
          return new Item(new JDecimal(d3)); // TODO: memory
        }
        catch (ArithmeticException e)
        {
          // TODO: need +INF, -INF, and NaN
          return Item.nil;
        }
      }
      default :
        throw new RuntimeException("invalid op:" + op);
    }
    return new Item(new JLong(n3));
  }

  /**
   * @param n1
   * @param n2
   * @return
   */
  private Item doubleEval(double n1, double n2)
  {
    double n3;
    switch (op)
    {
      case PLUS : {
        n3 = n1 + n2;
        break;
      }
      case MINUS : {
        n3 = n1 - n2;
        break;
      }
      case MULTIPLY : {
        n3 = n1 * n2;
        break;
      }
      case DIVIDE : {
        n3 = n1 / n2;
        break;
      }
      default :
        throw new RuntimeException("invalid op:" + op);
    }
    return new Item(new JDouble(n3)); // TODO: memory!
  }
}
