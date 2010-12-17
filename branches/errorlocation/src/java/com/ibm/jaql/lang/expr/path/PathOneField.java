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

import com.ibm.jaql.json.schema.Schema;
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
import com.ibm.jaql.util.FastPrinter;


/** e.g. .a as used is ${.a,.b} */;
public class PathOneField extends PathFields
{

  /**
   * @param exprs
   */
  public PathOneField(Expr[] exprs)
  {
    super(exprs);
  }

  /**
   * @param name
   */
  public PathOneField(Expr name)
  {
    super(name,new PathReturn());
  }

  /**
   * @return the field name 
   */
  public Expr nameExpr()
  {
	  return exprs[0];
  }
  
  
  /**
   * Return the mapping table.
   */
  @Override
  public MappingTable getMappingTable()
  {
	  MappingTable mt = new MappingTable();
	  
	  if (!(nameExpr() instanceof ConstExpr))
		  return mt;
	  JsonString name = (JsonString)((ConstExpr)nameExpr()).value;

	  //Find the iteration variable used outside the PathOneField
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
	  PathFieldValue pfvL = new PathFieldValue(new ConstExpr(name));
	  PathExpr peL = new PathExpr(veL, pfvL);

	  //Construct the R.H.S of the mapping (pathExpr)
	  VarMap vm = new VarMap();
	  VarExpr veR = new VarExpr(bindVar);
	  PathFieldValue pfvR = new PathFieldValue(new ConstExpr(name), exprs[1].clone(vm));
	  PathExpr peR = new PathExpr(veR, pfvR);
	  
	  //exprs[1] may recursively contain PathRecord expression
	  //Case I: If the inner PathRecord does not contain PathNotFields, then the details of this inner PathRecord is not important.
	  //         we replace it with PathReturn to avoid the overhead of re-computing them if Filter is pushed down.
	  //Case II: If the inner PathRecord contains PathNotFields, then it is not safe to push predicates on this field
	  ExprWalker walker = new PostOrderExprWalker();;
	  walker.reset(peR);
	  Expr e = null;
	  boolean safeMapping = true;
	  while ((e = walker.next()) != null)
	  {
		  if (e instanceof PathNotFields)
			  safeMapping = false; 
		  else if (e instanceof PathRecord)
			  e.replaceInParent(new PathReturn());
	  }
	  
	  //Add the mapping record
	  mt.add(peL, peR, safeMapping);
	  return mt;
  }
  
  
  /**
   * 
   */
  protected void decompileRaw(FastPrinter exprText, HashSet<Var> capturedVars, boolean emitLocation)
  throws Exception
  {
    exprText.print(".(");
    exprs[0].decompile(exprText, capturedVars,emitLocation);
    exprText.print(")");
    exprs[1].decompile(exprText, capturedVars,emitLocation);
  }

  /* (non-Javadoc)
   * @see com.ibm.jaql.lang.expr.core.PathFields#matches(com.ibm.jaql.lang.core.Context, com.ibm.jaql.json.type.JString)
   */
  @Override
  public boolean matches(Context context, JsonString name) throws Exception
  {
    JsonString n = (JsonString)exprs[0].eval(context);
    if( n == null )
    {
      return false;
    }
    return n.equals(name);
  }
  
  // -- schema ------------------------------------------------------------------------------------
  
  @Override
  public PathStepSchema getSchema(Schema inputSchema)
  {
    return staticResolveField(inputSchema, exprs[0], nextStep());
  }
}
