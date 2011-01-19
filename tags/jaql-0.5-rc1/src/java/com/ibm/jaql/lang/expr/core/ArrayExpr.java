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

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Map;

import com.ibm.jaql.json.schema.ArraySchema;
import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.type.BufferedJsonArray;
import com.ibm.jaql.json.type.JsonArray;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.core.VarMap;
import com.ibm.jaql.lang.expr.metadata.MappingTable;
import com.ibm.jaql.lang.expr.path.PathExpr;
import com.ibm.jaql.lang.expr.path.PathIndex;
import com.ibm.jaql.lang.expr.path.PathReturn;
import com.ibm.jaql.lang.expr.path.PathStep;
import com.ibm.jaql.util.Bool3;
import com.ibm.jaql.util.FastPrinter;

/**
 * 
 */
public class ArrayExpr extends IterExpr
{
  // Runtime state:
  protected BufferedJsonArray tuple;
  
  
  /**
   * @param exprs
   */
  public ArrayExpr(Expr ... exprs)
  {
    super(exprs);
  }

  /**
   * 
   */
  public ArrayExpr()
  {
    super(NO_EXPRS);
  }

  /**
   * @param exprs
   */
  public ArrayExpr(ArrayList<Expr> exprs)
  {
    super(exprs.toArray(new Expr[exprs.size()]));
  }

  
  /**
   * Return the mapping table.
   */
  @Override
  public MappingTable getMappingTable()
  {
	MappingTable mt = new MappingTable();
  		
	for (int i=0; i<exprs.length; i++)
	{
		MappingTable child_table = exprs[i].getMappingTable();
		VarExpr ve = new VarExpr(new Var(MappingTable.DEFAULT_PIPE_VAR));
		PathIndex pi = new PathIndex(new ConstExpr(i));
		PathExpr pe = new PathExpr(ve, pi);
		
		if (exprs[i] instanceof RecordExpr)
		{
			//Add the mapping at the field level: We need to change the AfterExpr in the mappings returned from the RecordExpr
			Enumeration<Expr> e = child_table.KeyEnum();
			while (e.hasMoreElements())
			{
				Expr after_expr = e.nextElement();
				Expr before_expr = child_table.BeforeExpr(after_expr);
				boolean safetyFlag = child_table.isSafeToMapExpr(after_expr);
				  
				if (after_expr instanceof PathExpr)
				{
					VarMap vm = new VarMap();
					ve = new VarExpr(new Var(MappingTable.DEFAULT_PIPE_VAR));
					PathStep ps = (PathStep)(((PathExpr)(after_expr)).firstStep()).clone(vm);
					pi = new PathIndex(new ConstExpr(i), ps);
					pe = new PathExpr(ve, pi);
					mt.add(pe, before_expr, safetyFlag);					  
				}	
			}
		}
		else   
			mt.add(pe, exprs[i], child_table.isSafeToMapAll());
	}

	//Add the mapping at the array level
	VarExpr ve = new VarExpr(new Var(MappingTable.DEFAULT_PIPE_VAR));
	PathExpr pe = new PathExpr(ve, new PathReturn());
	mt.add(pe, this, false);                        //TODO: Is it really "false"  

	return mt;
  }
  

  public Schema getSchema()
  {
    // TODO: cache?
    Schema[] schemata = new Schema[exprs.length]; 
    for (int i=0; i<exprs.length; i++)
    {
      schemata[i] = exprs[i].getSchema();
    }
    return new ArraySchema(schemata);
  }
  
  /**
   * 
   */
  @Override
  public Bool3 evaluatesChildOnce(int i)
  {
    return Bool3.TRUE;
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
  public void decompile(FastPrinter exprText, HashSet<Var> capturedVars)
      throws Exception
  {
    exprText.print("[");
    int n = exprs.length;
    String sep = "";
    for (int i = 0; i < n; i++)
    {
      exprText.print(sep);
      exprs[i].decompile(exprText, capturedVars);
      sep = ", ";
    }
    exprText.print("]");
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.IterExpr#eval(com.ibm.jaql.lang.core.Context)
   */
  public JsonValue eval(Context context) throws Exception // TODO: generalize for other tuple-like exprs?
  {
    if (exprs.length == 0)
    {
      return JsonArray.EMPTY;
    }
    if( tuple == null )
    {
      tuple = new BufferedJsonArray(exprs.length);
    }
    for (int i = 0; i < exprs.length; i++)
    {
      JsonValue value = exprs[i].eval(context);
      tuple.set(i, value);
    }
    return tuple;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.IterExpr#iter(com.ibm.jaql.lang.core.Context)
   */
  public JsonIterator iter(final Context context) throws Exception
  {
    if (exprs.length == 0)
    {
      return JsonIterator.EMPTY;
    }
    return new JsonIterator() {
      int i = 0;

      public boolean moveNext() throws Exception
      {
        while (true)
        {
          if (i == exprs.length)
          {
            return false;
          }
          Expr expr = exprs[i++];
          currentValue = expr.eval(context);
          return true;
        }
      }
    };
  }
}
