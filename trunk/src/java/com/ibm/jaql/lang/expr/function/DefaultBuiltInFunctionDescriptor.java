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
package com.ibm.jaql.lang.expr.function;

import java.lang.reflect.Constructor;

import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.lang.core.Env;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.MacroExpr;
import com.ibm.jaql.lang.util.JaqlUtil;

/** Default descriptor for built-in functions. */
public class DefaultBuiltInFunctionDescriptor implements BuiltInFunctionDescriptor
{
  // Parij = function that takes between i and j arguments, j=u stand for unlimited
  public static class Par00 extends DefaultBuiltInFunctionDescriptor { public Par00(String name, Class<? extends Expr> c) { super(name,c,0,0); } };
  public static class Par01 extends DefaultBuiltInFunctionDescriptor { public Par01(String name, Class<? extends Expr> c) { super(name,c,0,1); } };
  public static class Par02 extends DefaultBuiltInFunctionDescriptor { public Par02(String name, Class<? extends Expr> c) { super(name,c,0,2); } };
  public static class Par03 extends DefaultBuiltInFunctionDescriptor { public Par03(String name, Class<? extends Expr> c) { super(name,c,0,3); } };
  public static class Par04 extends DefaultBuiltInFunctionDescriptor { public Par04(String name, Class<? extends Expr> c) { super(name,c,0,4); } };
  public static class Par05 extends DefaultBuiltInFunctionDescriptor { public Par05(String name, Class<? extends Expr> c) { super(name,c,0,5); } };
  public static class Par11 extends DefaultBuiltInFunctionDescriptor { public Par11(String name, Class<? extends Expr> c) { super(name,c,1,1); } };
  public static class Par12 extends DefaultBuiltInFunctionDescriptor { public Par12(String name, Class<? extends Expr> c) { super(name,c,1,2); } };
  public static class Par13 extends DefaultBuiltInFunctionDescriptor { public Par13(String name, Class<? extends Expr> c) { super(name,c,1,3); } };
  public static class Par14 extends DefaultBuiltInFunctionDescriptor { public Par14(String name, Class<? extends Expr> c) { super(name,c,1,4); } };
  public static class Par15 extends DefaultBuiltInFunctionDescriptor { public Par15(String name, Class<? extends Expr> c) { super(name,c,1,5); } };
  public static class Par22 extends DefaultBuiltInFunctionDescriptor { public Par22(String name, Class<? extends Expr> c) { super(name,c,2,2); } };
  public static class Par23 extends DefaultBuiltInFunctionDescriptor { public Par23(String name, Class<? extends Expr> c) { super(name,c,2,3); } };
  public static class Par24 extends DefaultBuiltInFunctionDescriptor { public Par24(String name, Class<? extends Expr> c) { super(name,c,2,4); } };
  public static class Par25 extends DefaultBuiltInFunctionDescriptor { public Par25(String name, Class<? extends Expr> c) { super(name,c,2,5); } };
  public static class Par33 extends DefaultBuiltInFunctionDescriptor { public Par33(String name, Class<? extends Expr> c) { super(name,c,3,3); } };
  public static class Par34 extends DefaultBuiltInFunctionDescriptor { public Par34(String name, Class<? extends Expr> c) { super(name,c,3,4); } };
  public static class Par35 extends DefaultBuiltInFunctionDescriptor { public Par35(String name, Class<? extends Expr> c) { super(name,c,3,5); } };
  public static class Par44 extends DefaultBuiltInFunctionDescriptor { public Par44(String name, Class<? extends Expr> c) { super(name,c,4,4); } };
  public static class Par45 extends DefaultBuiltInFunctionDescriptor { public Par45(String name, Class<? extends Expr> c) { super(name,c,4,5); } };
  public static class Par55 extends DefaultBuiltInFunctionDescriptor { public Par55(String name, Class<? extends Expr> c) { super(name,c,5,5); } };
  public static class Par0u extends DefaultBuiltInFunctionDescriptor { public Par0u(String name, Class<? extends Expr> c) { super(name,c,0); } };
  public static class Par1u extends DefaultBuiltInFunctionDescriptor { public Par1u(String name, Class<? extends Expr> c) { super(name,c,1); } };
  public static class Par2u extends DefaultBuiltInFunctionDescriptor { public Par2u(String name, Class<? extends Expr> c) { super(name,c,2); } };

  private String name;
  private Class<? extends Expr> implementingClass;
  private JsonValueParameters parameters;
  private Schema resultSchema;
  private Constructor<? extends Expr> constructor;
  
  /** Construct a function descriptor from detailed information about the function. */
  public DefaultBuiltInFunctionDescriptor(String name, Class<? extends Expr> implementingClass,
      JsonValueParameters parameters, Schema resultSchema)
  {
    init(name, implementingClass);
    this.parameters = parameters;
    this.resultSchema = resultSchema;
  }
  
  /** Construct a function descriptor for a function that takes between <code>min</code> 
   * and <code>max</code> arguments of arbitrary type. */
  public DefaultBuiltInFunctionDescriptor(String name, Class<? extends Expr> implementingClass,
      int min, int max)
  {
    init(name, implementingClass);
    JsonString[] names = new JsonString[max];
    for (int i=0; i<max; i++)
    {
      names[i] = new JsonString("arg" + i);
    }
    
    parameters = new JsonValueParameters(names, min, null);
    resultSchema = SchemaFactory.anySchema();
  }
  
  /** Construct a function descriptor for a function that takes at least <code>min</code> 
   * arguments of arbitrary type (without a restiction on the maximum number of arguments). */
  public DefaultBuiltInFunctionDescriptor(String name, Class<? extends Expr> implementingClass,
      int min)
  {
    init(name, implementingClass);
    JsonValueParameter[] pars = new JsonValueParameter[min+1];
    for (int i=0; i<min; i++)
    {
      pars[i] = new JsonValueParameter("arg" + i);
    }
    pars[min] = new JsonValueParameter("arg" + min, SchemaFactory.anySchema(), true);
               
    parameters = new JsonValueParameters(pars);
    resultSchema = SchemaFactory.anySchema();
  }
  
  private void init(String name, Class<? extends Expr> implementingClass)
  {
    this.name = name;
    this.implementingClass = implementingClass;
    try
    {
      this.constructor = implementingClass.getConstructor(Expr[].class);
    } catch (Exception e)
    {
      throw JaqlUtil.rethrow(e);
    }
  }

  @Override
  public String getName()
  {
    return name;
  }

  @Override
  public JsonValueParameters getParameters()
  {
    return parameters;
  }

  @Override
  public Schema getSchema()
  {
    return resultSchema;
  }
  
  @Override
  public Class<? extends Expr> getImplementingClass()
  {
    return implementingClass;
  }
  
  @Override 
  public Expr construct(Expr[] positionalArgs) 
  {
    try
    {
      Expr e = constructor.newInstance(new Object[] { positionalArgs });
      if (e instanceof MacroExpr)
      {
        // should be rare because variables are usually inlined
        e = ((MacroExpr)e).expand(new Env(null)); // FIXME: ksb: This doesn't look healthy.  A new Env? No Context? Maybe expand shouldn't get an Env (which means eliminating env.makeVar())
      }
      return e;
    } catch (Exception e)
    {
      throw JaqlUtil.rethrow(e); 
    }
  }
}
