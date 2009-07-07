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

import com.ibm.jaql.json.type.JsonDate;
import com.ibm.jaql.json.type.JsonDecimal;
import com.ibm.jaql.json.type.JsonDouble;
import com.ibm.jaql.json.type.JsonLong;
import com.ibm.jaql.json.type.JsonNumber;
import com.ibm.jaql.json.type.JsonNumeric;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
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
      JsonValue t = ce.value;
      if (t instanceof JsonNumeric)
      {
        ((JsonNumeric) t).negate();
        return expr;
      }
    }
    return new MathExpr(MathExpr.MINUS, new ConstExpr(JsonLong.ZERO), expr);
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
  public JsonValue eval(Context context) throws Exception
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
    if (value1 instanceof JsonLong && value2 instanceof JsonLong)
    {
      long n1 = ((JsonLong) value1).value;
      long n2 = ((JsonLong) value2).value;
      return longEval(n1, n2);
    }
    else if (value1 instanceof JsonDate && value2 instanceof JsonDate)
    {
      long n1 = ((JsonDate) value1).millis;
      long n2 = ((JsonDate) value2).millis;
      return longEval(n1, n2);
    }
    else if (value1 instanceof JsonString || value2 instanceof JsonString)
    {
      if (op != PLUS) // TODO: use a different symbol or function for string concat? javascript uses +
      {
        throw new RuntimeException("invalid operator on strings");
      }
      // TODO: memory
      JsonString text1 = (JsonString) value1;
      JsonString text2 = (JsonString) value2;
      byte[] buf = new byte[text1.getLength() + text2.getLength()];
      System.arraycopy(text1.getInternalBytes(), 0, buf, 0, text1.getLength());
      System.arraycopy(text2.getInternalBytes(), 0, buf, text1.getLength(), text2
          .getLength());
      return new JsonString(buf);
    }
    else if (value1 instanceof JsonDouble || value2 instanceof JsonDouble)
    {
      double d1 = ((JsonNumeric) value1).doubleValue();
      double d2 = ((JsonNumeric) value2).doubleValue();
      return doubleEval(d1, d2);
    }
    else
    {
      BigDecimal n1 = ((JsonNumber) value1).decimalValue();
      BigDecimal n2 = ((JsonNumber) value2).decimalValue();
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
            return null;
          }
          break;
        }
        default :
          throw new RuntimeException("invalid op:" + op);
      }
      return new JsonDecimal(n3);
    }
  }

  /**
   * @param n1
   * @param n2
   * @return
   */
  private JsonValue longEval(long n1, long n2)
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
          return new JsonDecimal(d3); // TODO: memory
        }
        catch (ArithmeticException e)
        {
          // TODO: need +INF, -INF, and NaN
          return null;
        }
      }
      default :
        throw new RuntimeException("invalid op:" + op);
    }
    return new JsonLong(n3);
  }

  /**
   * @param n1
   * @param n2
   * @return
   */
  private JsonValue doubleEval(double n1, double n2)
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
    return new JsonDouble(n3); // TODO: memory!
  }

  public static JsonValue divide(JsonValue x, JsonValue y)
  {
    if( x == null || y == null )
    {
      return null;
    }
    
    if( x instanceof JsonDouble )
    {
      JsonDouble dx = (JsonDouble)x;
      JsonDouble dy = (JsonDouble)y;
      double div = dx.value / dy.value;
      return new JsonDouble(div);
    }
    else
    {
      BigDecimal dx = ((JsonNumber)x).decimalValue();
      BigDecimal dy = ((JsonNumber)y).decimalValue();
      try
      {
        // dz = dx / dy
        BigDecimal dz = dx.divide(dy, MathContext.DECIMAL128);
        return new JsonDecimal(dz); // TODO: memory
      }
      catch (ArithmeticException e)
      {
        // TODO: need +INF, -INF, and NaN
        return null;
      }
    }
  }
}
