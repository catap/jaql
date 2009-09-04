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

import com.ibm.jaql.json.schema.ArraySchema;
import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.schema.SchemaTransformation;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.JsonComparator;
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.expr.function.JaqlFunction;
import com.ibm.jaql.lang.util.JsonSorter;
import com.ibm.jaql.util.Bool3;
import static com.ibm.jaql.json.type.JsonType.*;

/**
 * 
 */
public class SortExpr extends IterExpr
{
  /**
   * @param exprs: Expr input, Expr cmp
   */
  public SortExpr(Expr[] exprs)
  {
    super(exprs);
  }

  // exprs[0] is a BindingExpr b
  public SortExpr(Expr input, Expr cmp)
  {
    super(input, cmp);
  }
  
  public Expr cmpExpr()
  {
    return exprs[1];
  }

  //  public SortExpr(Env env, String varName, Expr inputExpr)
  //  {
  //    this.input = new Binding(Binding.IN_BINDING, env.scope(varName), inputExpr);
  //  }
  //
  //  public void addBy(Expr by, boolean asc1)
  //  {
  //    byExprs.add(by);
  //    boolean[] a = new boolean[asc.length + 1];
  //    System.arraycopy(asc, 0, a, 0, asc.length);
  //    a[asc.length] = asc1;
  //    asc = a;
  //  }
  //
  //  public void done(Env env)
  //  {
  //    env.unscope(input.var);
  //  }

  /**
   * 
   */
  @Override
  public Bool3 evaluatesChildOnce(int i)
  {
    if( i == 0 )
    {
      return Bool3.TRUE;
    }
    return Bool3.FALSE;
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
    exprs[0].decompile(exprText, capturedVars);
    exprText.print("\n  -> sort using (");
    exprs[1].decompile(exprText, capturedVars);
    exprText.print(")");
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.IterExpr#iter(com.ibm.jaql.lang.core.Context)
   */
  public JsonIterator iter(final Context context) throws Exception
  {
    JaqlFunction cmpFn = (JaqlFunction)cmpExpr().eval(context);
    if( cmpFn.getParameters().numParameters() != 1 || !(cmpFn.body() instanceof CmpExpr) )
    {
      throw new RuntimeException("invalid comparator function");
    }
    Var cmpVar = cmpFn.getParameters().get(0).getVar();
    CmpExpr cmp = (CmpExpr)cmpFn.body();
    JsonComparator comparator = cmp.getComparator(context);

    final JsonSorter temp = new JsonSorter(comparator);

    JsonIterator iter = exprs[0].iter(context);
    if (iter.isNull())
    {
      return JsonIterator.NULL;
    }
    for (JsonValue value : iter)
    {
      cmpVar.setValue(value);
      JsonValue byValue = cmp.eval(context);
      temp.add(byValue, value);
    }

    temp.sort();

//    final Item[] byItems = new Item[nby];
//    for (int i = 0; i < nby; i++)
//    {
//      byItems[i] = new Item();
//    }

    return temp.iter();
  }
  
  @Override
  public Schema getSchema()
  {
    Schema in = exprs[0].getSchema();
    Schema out = SchemaTransformation.restrictToArray(in);
    
    // handle the case where input is null or non-array
    if (out == null)
    {
      if (in.is(NULL).maybe())
      {
        return SchemaFactory.nullSchema();
      }
      throw new IllegalArgumentException("sort expects arrays as input");
    }

    if (out.isEmpty(ARRAY).always())
    {
      out = SchemaFactory.emptyArraySchema();
    }
    else
    {
      // get rid of order
      out = new ArraySchema(out.elements(), out.minElements(), out.maxElements());
    }
    
    // handle nulls (when input can be an array)
    if (in.is(NULL).maybe())
    { 
      out = SchemaTransformation.addNullability(out);
    }
    
    // that's it
    return out;
  }
}
