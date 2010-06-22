/*
 * Copyright (C) IBM Corp. 2009.
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
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.expr.metadata.MappingTable;
import com.ibm.jaql.lang.expr.path.PathExpr;
import com.ibm.jaql.util.Bool3;
import static com.ibm.jaql.json.type.JsonType.*;

public final class TransformExpr extends IterExpr
{
  /**
   * BindingExpr inExpr, Expr projection
   * 
   * @param exprs
   */
  public TransformExpr(Expr[] exprs)
  {
    super(exprs);
  }

  /**
   * @param inBinding
   * @param projection
   */
  public TransformExpr(BindingExpr inBinding, Expr projection)
  {
    super(inBinding, projection);
  }

  /**
   * @param mapVar
   * @param inExpr
   * @param projection
   */
  public TransformExpr(Var mapVar, Expr inExpr, Expr projection)
  {
    super(new BindingExpr(BindingExpr.Type.IN, mapVar, null, inExpr),
        projection);
  }
  
  /**
   * @return
   */
  public BindingExpr binding()
  {
    return (BindingExpr) exprs[0];
  }

  /**
   * @return
   */
  public Var var()
  {
    return binding().var;
  }

  /**
   * @return
   */
  public Expr projection()
  {
    return exprs[1];
  }

  /**
   * Return the mapping table.
   */
  @Override
  public MappingTable getMappingTable()
  {
	  MappingTable mt = new MappingTable();
	  if ( (projection() instanceof RecordExpr) || (projection() instanceof PathExpr) ||
			  (projection() instanceof VarExpr) || (projection() instanceof ArrayExpr) ||
			  (projection() instanceof ConstExpr))
	  {	
		  mt.addAll(projection().getMappingTable());
	  }
	  else
	  {
		  VarExpr ve = new VarExpr(new Var(MappingTable.DEFAULT_PIPE_VAR));  
		  mt.add(ve, projection(), false);
	  }
	  mt.addUnsafeMappingRecord();
	  
	  return mt;
  }
  
  
  @Override
  public Schema getSchema()
  {
    Schema in = binding().getSchema(); // binds variable for projection
    if (in.isEmpty(ARRAY,NULL).always())
    {
      return SchemaFactory.emptyArraySchema();
    }
    return new ArraySchema(null, projection().getSchema());
  }


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

  /**
   * This expression can be applied in parallel per partition of child i.
   */
  @Override
  public boolean isMappable(int i)
  {
    return i == 0;
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
    BindingExpr b = binding();
    b.inExpr().decompile(exprText, capturedVars);
    exprText.print("\n-> " + kw("transform") + " " + kw("each") + " ");
    exprText.print(b.var.taggedName());
    exprText.print(" (");
    projection().decompile(exprText, capturedVars);
    exprText.print(")");
    capturedVars.remove(b.var);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.IterExpr#iter(com.ibm.jaql.lang.core.Context)
   */
  public JsonIterator iter(final Context context) throws Exception
  {
    final BindingExpr inBinding = binding();
    final Expr proj = projection();
    final JsonIterator inIter = inBinding.iter(context);

    return new JsonIterator() {
      public boolean moveNext() throws Exception
      {
        if (inIter.moveNext()) { // sets inBinding.var
          currentValue = proj.eval(context);
          return true;
        }
        return false;
      }
    };
  }

}
