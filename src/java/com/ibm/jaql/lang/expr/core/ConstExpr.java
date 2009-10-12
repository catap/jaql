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
import java.util.Map;

import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.type.JsonArray;
import com.ibm.jaql.json.type.JsonDate;
import com.ibm.jaql.json.type.JsonLong;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonRegex;
import com.ibm.jaql.json.type.JsonUtil;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.core.VarMap;
import com.ibm.jaql.lang.expr.function.Function;

/**
 * 
 */
public final class ConstExpr extends Expr
{
  public JsonValue value;

  /**
   * @param value
   */
  public ConstExpr(JsonValue value)
  {
    super(NO_EXPRS);
    this.value = value;
  }


  public ConstExpr(long v)
  {
    this(new JsonLong(v));
  }
  
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
  public void decompile(PrintStream exprText, HashSet<Var> capturedVars)
      throws Exception
  {
    boolean annotate = 
      value instanceof JsonArray    || 
      value instanceof JsonRecord   || 
      value instanceof Function || // TODO: JValue.getType().isExtendedJson()
      value instanceof JsonDate     || // TODO: parser should recognize constructors and eval during parse
      value instanceof JsonRegex;
    if (annotate) exprText.print("const("); // FIXME: remove
    JsonUtil.print(exprText, value, 2);
    if (annotate) exprText.print(")");// FIXME: remove
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#eval(com.ibm.jaql.lang.core.Context)
   */
  public JsonValue eval(Context context) throws Exception
  {
    return value;
  }

  @Override
  public JsonValue compileTimeEval() throws Exception
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
  
  public Schema getSchema()
  {
    return SchemaFactory.schemaOf(value);
  }
}
