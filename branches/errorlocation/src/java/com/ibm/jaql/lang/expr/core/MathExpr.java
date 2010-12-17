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

import static com.ibm.jaql.json.type.JsonType.NULL;

import java.math.BigDecimal;
import java.math.MathContext;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import com.ibm.jaql.json.schema.OrSchema;
import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.schema.SchemaTransformation;
import com.ibm.jaql.json.type.JsonDecimal;
import com.ibm.jaql.json.type.JsonDouble;
import com.ibm.jaql.json.type.JsonLong;
import com.ibm.jaql.json.type.JsonNumber;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonType;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.type.MutableJsonDecimal;
import com.ibm.jaql.json.type.MutableJsonDouble;
import com.ibm.jaql.json.type.MutableJsonLong;
import com.ibm.jaql.json.type.MutableJsonString;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.core.VarMap;
import com.ibm.jaql.lang.expr.metadata.MappingTable;
import com.ibm.jaql.util.Bool3;
import com.ibm.jaql.util.FastPrinter;

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
      if (t instanceof JsonLong)
      {
        return new ConstExpr(new JsonLong(-((JsonLong) t).get()));
      }
      if (t instanceof MutableJsonLong)
      {
        ((MutableJsonLong) t).negate();
        return expr;
      }
      if (t instanceof JsonDouble)
      {
        return new ConstExpr(new JsonDouble(-((JsonDouble) t).get()));
      }
      if (t instanceof MutableJsonDouble)
      {
        ((MutableJsonDouble) t).negate();
        return expr;
      }
      if (t instanceof JsonDecimal)
      {
        return new ConstExpr(new JsonDecimal(((JsonDecimal) t).get().negate()));
      }
      if (t instanceof MutableJsonDecimal)
      {
        ((MutableJsonDecimal) t).negate();
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
   * @see com.ibm.jaql.lang.expr.core.Expr#decompile(java.io.PrintStream,
   *      java.util.HashSet)
   */
  @Override
  protected void decompileRaw(FastPrinter exprText, HashSet<Var> capturedVars, boolean emitLocation)
      throws Exception
  {
    exprText.print("(");
    exprs[0].decompile(exprText, capturedVars,emitLocation);
    exprText.print(")" + OP_STR[op] + "(");
    exprs[1].decompile(exprText, capturedVars,emitLocation);
    exprText.print(")");
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#clone(com.ibm.jaql.lang.core.VarMap)
   */
  public Expr clone(VarMap varMap)
  {
    return cloneOrigin(new MathExpr(op, exprs[0].clone(varMap), exprs[1].clone(varMap)));
  }

  
  @Override
  public Bool3 evaluatesChildOnce(int i)
  {
    return Bool3.TRUE;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#eval(com.ibm.jaql.lang.core.Context)
   */
  @Override
protected JsonValue evalRaw(Context context) throws Exception
  {
    JsonValue value1 = exprs[0].eval(context);
    JsonValue value2 = exprs[1].eval(context);
    return eval(value1, value2, op);
  }
  
  public static JsonValue eval(JsonValue value1, JsonValue value2, int op)
  {
    // changes here should be reflected in getSchema() below
    if (value1 == null || value2 == null)
    {
      return null;
    }
    switch (value1.getType())
    {
    case LONG:
      switch (value2.getType()) {
      case LONG:
        return longEval((JsonNumber)value1, (JsonNumber)value2, op);
      case DOUBLE:
        return doubleEval((JsonNumber)value1, (JsonNumber)value2, op);
      case DECFLOAT:
        return decimalEval((JsonNumber)value1, (JsonNumber)value2, op);
      }
      break;
    case DOUBLE:
      switch (value2.getType()) {
      case LONG:
        return doubleEval((JsonNumber)value1, (JsonNumber)value2, op);
      case DOUBLE:
        return doubleEval((JsonNumber)value1, (JsonNumber)value2, op);
      case DECFLOAT:
        return decimalEval((JsonNumber)value1, (JsonNumber)value2, op);
      }
      break;
    case DECFLOAT:
      switch (value2.getType()) {
      case LONG:
        return decimalEval((JsonNumber)value1, (JsonNumber)value2, op);
      case DOUBLE:
        return decimalEval((JsonNumber)value1, (JsonNumber)value2, op);
      case DECFLOAT:
        return decimalEval((JsonNumber)value1, (JsonNumber)value2, op);
      }
      break;
    case STRING:
      if (value2.getType() == JsonType.STRING && op==PLUS)
      {
        JsonString text1 = (JsonString) value1;
        JsonString text2 = (JsonString) value2;
        MutableJsonString result = new MutableJsonString();
        result.ensureCapacity(text1.bytesLength() + text2.bytesLength());
        byte[] buf = result.get();
        text1.writeBytes(buf, 0, text1.bytesLength());
        text2.writeBytes(buf, text1.bytesLength(), text2.bytesLength());
        result.set(buf, text1.bytesLength() + text2.bytesLength());
        return result;
      }
      break;
    }
    
    // if we reach this point, we cannot apply the op
    throw new RuntimeException("Operation " + OP_STR[op] + " not defined for input types " 
        + value1.getType() + " and " + value2.getType());
  }


  private static JsonValue longEval(JsonNumber v1, JsonNumber v2, int op)
  {
    return longEval(v1.longValue(), v2.longValue(), op);
  }
  
  /**
   * @param n1
   * @param n2
   * @return
   */
  private static JsonValue longEval(long n1, long n2, int op)
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
        n3 = n1/n2;
        break;
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
  private static JsonValue doubleEval(JsonNumber v1, JsonNumber v2, int op)
  {
    double n1 = v1.doubleValue();
    double n2 = v2.doubleValue();
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

  private static JsonValue decimalEval(JsonNumber v1, JsonNumber v2, int op)
  {
    BigDecimal n1 = v1.decimalValue();
    BigDecimal n2 = v2.decimalValue();
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

  // -- schema ------------------------------------------------------------------------------------
  
  /** Returns the number type that results by promoting the specified values to a common
   * number type or null if impossible. */
  public static JsonType promote(JsonType type1, JsonType type2)
  {
    switch (type1)
    {
    case LONG:
      switch (type2)
      {
      case LONG:
        return JsonType.LONG;
      case DOUBLE:
        return JsonType.DOUBLE;
      case DECFLOAT:
        return JsonType.DECFLOAT;
      }
      break;
    case DOUBLE:
      switch (type2)
      {
      case LONG:
      case DOUBLE:
        return JsonType.DOUBLE;
      case DECFLOAT:
        return JsonType.DECFLOAT;
      }
      break;  
    case DECFLOAT:
      switch (type2)
      {
      case LONG:
      case DOUBLE:
      case DECFLOAT:
        return JsonType.DECFLOAT;
      }
      break;
    }
    return null;
  }
  
  /** Returns the number type that results by promoting the specified values to a common type. */
  public static JsonType promote(JsonNumber value1, JsonNumber value2)
  {
    return promote(value1.getType(), value2.getType());
  }
  
  /** Returns a schema that matches all the number types that results by promoting the number
   * types in the specified input schemata to a common type. If any input can be null, adds 
   * nullability. Returns null if inputs neither match null or any number. */
  public static Schema promote(Schema s1, Schema s2)
  {
    // check for nulls
    boolean isNullable = s1.is(NULL).maybe() || s2.is(NULL).maybe();
    s1 = SchemaTransformation.removeNullability(s1);
    s2 = SchemaTransformation.removeNullability(s2);
    if (s1==null || s2==null)
    {
      return SchemaFactory.nullSchema();
    }
    
    // check for numeric types
    boolean s1l = s1.is(JsonType.LONG).maybe();
    boolean s1d = s1.is(JsonType.DOUBLE).maybe();
    boolean s1m = s1.is(JsonType.DECFLOAT).maybe();
    boolean s2l = s2.is(JsonType.LONG).maybe();
    boolean s2d = s2.is(JsonType.DOUBLE).maybe();
    boolean s2m = s2.is(JsonType.DECFLOAT).maybe();
    if (!(s1l || s1d || s1m) || !(s2l || s2d || s2m)) return null; // non-number inputs
    List<Schema> result = new ArrayList<Schema>(4);
    if (s1l && s2l) { 
      result.add(SchemaFactory.longSchema());
    }
    if (((s1l || s1d) && s2d) || (s1d && (s2l || s2d))) {
      result.add(SchemaFactory.doubleSchema());
    }
    if (s1m || s2m)
    {
      result.add(SchemaFactory.decfloatSchema());
    }
    assert result.size() > 0;
    if (isNullable)
    {
      result.add(SchemaFactory.nullSchema());
    }
    return OrSchema.make(result);
  }
  
  
  /**
   * Return the mapping table.
   */
  @Override
  public MappingTable getMappingTable()
  {
	  MappingTable mt = new MappingTable();
	  boolean safety_flag = (exprs[0].getMappingTable().isSafeToMapAll()) && (exprs[1].getMappingTable().isSafeToMapAll());  
	  VarExpr ve= new VarExpr(new Var(MappingTable.DEFAULT_PIPE_VAR));	  
	  mt.add(ve, this, safety_flag);            
	  return mt;
  }
  
  
  @Override
  public Schema getSchema()
  {
    Schema s1 = exprs[0].getSchema();
    Schema s2 = exprs[1].getSchema();
    
    Schema result = promote(s1, s2);
    if (s1.is(JsonType.STRING).maybe() && s2.is(JsonType.STRING).maybe())
    {
      if (result == null)
      {
        return SchemaFactory.stringSchema();
      }
      else
      {
        return OrSchema.make(SchemaFactory.stringSchema(), result);
      }
    }
    
    if (result == null)
    {
      //throw new RuntimeException("Operation " + OP_STR[op] + " not defined for input types.");
      return SchemaFactory.anySchema();    
    }
    return result;
  }
}
