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
package com.ibm.jaql.lang.expr.path;

import java.util.HashSet;

import com.ibm.jaql.json.schema.RecordSchema;
import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.core.VarMap;
import com.ibm.jaql.lang.expr.core.ConstExpr;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.VarExpr;
import com.ibm.jaql.lang.expr.metadata.MappingTable;
import com.ibm.jaql.lang.walk.ExprWalker;
import com.ibm.jaql.lang.walk.PostOrderExprWalker;
import com.ibm.jaql.util.Bool3;
import com.ibm.jaql.util.FastPrinter;


/**
 *
 */
public class PathAllFields extends PathFields
{

  /**
   * @param exprs
   */
  public PathAllFields(Expr[] exprs)
  {
    super(exprs);
  }

  /**
   */
  public PathAllFields()
  {
    super(new PathReturn());
  }

  
  /**
   * Return the mapping table.
   * Since all fields are mapped, then we add a simple mapping record that should match (partially) with any thing: ($ --mapsTo--> $)
   */
  @Override
  public MappingTable getMappingTable()
  {
	  MappingTable mt = new MappingTable();
	  
	  //Find the iteration variable used outside the PathALLFields
	  Var bindVar = null;
	  Expr parent = this.parent(); 
	  if ((!(parent instanceof PathRecord)) || (!(parent.parent() instanceof PathExpr)))
		  return mt;
	  PathExpr peParent = (PathExpr)parent.parent();
	  VarExpr veParent = MappingTable.findVarInExpr(peParent.input());
	  if (veParent == null)
		  return mt;
	  else
		  bindVar = veParent.var();
		  
	  //Construct the L.H.S of the mapping	
	  VarExpr veL = new VarExpr(new Var(MappingTable.DEFAULT_PIPE_VAR));
	  PathExpr peL = new PathExpr(veL, new PathReturn());

	  //Construct the R.H.S of the mapping (pathExpr)
	  VarExpr veR = new VarExpr(bindVar);
	  PathExpr peR = new PathExpr(veR, new PathReturn());
	  
	  //Add the mapping record
	  mt.add(peL, peR, true);
	  return mt;
  }
  
  
  /**
   * 
   */
  protected void decompileRaw(FastPrinter exprText, HashSet<Var> capturedVars, boolean emitLocation)
  throws Exception
  {
    exprText.print("*");
    exprs[0].decompile(exprText, capturedVars,emitLocation);
  }

  /* (non-Javadoc)
   * @see com.ibm.jaql.lang.expr.core.PathFields#matches(com.ibm.jaql.lang.core.Context, com.ibm.jaql.json.type.JString)
   */
  @Override
  public boolean matches(Context context, JsonString name) throws Exception
  {
    return true;
  }
  
  //special case: resulting fields nested in PathStepSchema.schema
  public PathStepSchema getSchema(Schema inputSchema)
  {
    if (inputSchema instanceof RecordSchema)
    {
      return new PathStepSchema(inputSchema, Bool3.TRUE);
    }
    return new PathStepSchema(SchemaFactory.recordSchema(), Bool3.TRUE);
  }
}
