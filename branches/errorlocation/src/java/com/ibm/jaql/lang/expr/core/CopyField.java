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

import java.util.HashSet;

import com.ibm.jaql.json.type.BufferedJsonRecord;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.core.VarMap;
import com.ibm.jaql.lang.expr.metadata.MappingTable;
import com.ibm.jaql.lang.expr.path.PathExpr;
import com.ibm.jaql.lang.expr.path.PathFieldValue;
import com.ibm.jaql.lang.expr.path.PathReturn;
import com.ibm.jaql.lang.expr.path.PathStep;
import com.ibm.jaql.util.Bool3;
import com.ibm.jaql.util.FastPrinter;

// e.g., $.a
public class CopyField extends FieldExpr
{
  public static enum When
  {
    ALWAYS,
    DEFINED,
    NONNULL
  };
  
  protected When when;
  
  /**
   * 
   * @param exprs: [recExpr, nameExpr]
   */
  public CopyField(Expr[] exprs, When when)
  {
    super(exprs);
    this.when = when;
  }

  /**
   * 
   * @param recExpr
   * @param nameExpr
   */
  public CopyField(Expr recExpr, Expr nameExpr, When when)
  {
    super(recExpr, nameExpr);
    this.when = when;
  }

  /**
   * 
   * @param recExpr
   * @param name
   */
  public CopyField(Expr recExpr, String name, When when)
  {
    this(recExpr, new ConstExpr(new JsonString(name)), when);
  }

  /**
   * 
   * @param recExpr
   * @param name
   */
  public CopyField(Expr recExpr, JsonString name, When when)
  {
    this(recExpr, new ConstExpr(name), when);
  }

  /**
   * 
   * @param recVar
   * @param name
   */
  public CopyField(Var recVar, String name, When when)
  {
    this(new VarExpr(recVar), new ConstExpr(new JsonString(name)), when);
  }

  public final Expr recExpr()
  {
    return exprs[0];
  }

  public final Expr nameExpr()
  {
    return exprs[1];
  }

  /** When is the field constructed */
  public When getWhen()
  {
    return when;
  }

  public void setWhen(When when)
  {
    this.when = when;
  }


  @Override
  public Bool3 evaluatesChildOnce(int i)
  {
    return Bool3.TRUE;
  }

  /**
   * 
   */
  @Override
  protected void decompileRaw(FastPrinter exprText, HashSet<Var> capturedVars, boolean emitLocation)
      throws Exception
  {
    exprText.print("(");
    recExpr().decompile(exprText, capturedVars,emitLocation);
    exprText.print(").");
    exprText.print("(");
    nameExpr().decompile(exprText, capturedVars,emitLocation);
    exprText.print(")");
  }

  /**
   * 
   */
  @Override
  public void eval(Context context, BufferedJsonRecord outrec) throws Exception
  {
    JsonString name = (JsonString) nameExpr().eval(context);
    if( name == null )
    {
      return;
    }
    JsonRecord inrec = (JsonRecord) recExpr().eval(context);
    if (inrec != null)
    {
      JsonValue value = inrec.get(name, null);
      if( when == When.DEFINED )
      {
        if( value != null || inrec.containsKey(name) )
        {
          outrec.add(name, value);
        }
      }
      else if( when == When.NONNULL )
      {
        if( value != null )
        {
          outrec.add(name, value);
        }
      }
      else
      {
        assert when == When.ALWAYS;
        outrec.add(name, value);
      }
    }
    else if( when == When.ALWAYS ) // inrec==null
    {
      outrec.add(name, null);   // TODO: should this create the field?
    }
  }
  
  public JsonString staticName()
  {
    if (exprs[1] instanceof ConstExpr)
    {
      ConstExpr c = (ConstExpr) exprs[1];
      return (JsonString) c.value;
    }
    return null;
  }

  /**
   * 
   */
  @Override
  public Bool3 staticNameMatches(JsonString name)
  {
    if (exprs[1] instanceof ConstExpr)
    {
      ConstExpr c = (ConstExpr) exprs[1];
      JsonString text = (JsonString) c.value;
      if (text == null)
      {
        return Bool3.FALSE;
      }
      if (text.equals(name))
      {
        return Bool3.TRUE;
      }
      return Bool3.FALSE;
    }
    return Bool3.UNKNOWN;
  }

  public Expr toPathExpr()
  {
    return new PathExpr(exprs[0], new PathFieldValue(exprs[1], new PathReturn()));
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
	  
	  MappingTable child_table = recExpr().getMappingTable();	  
	  ConstExpr ce = new ConstExpr(staticName());
	  VarExpr ve = new VarExpr(new Var(MappingTable.DEFAULT_PIPE_VAR));
	  PathFieldValue pfv = new PathFieldValue(ce);
	  PathExpr pe = new PathExpr(ve, pfv);

	  if (recExpr() instanceof PathExpr) 
	  {
		  //Create a PathExpr equivalent to this CopyField
		  VarMap vm = new VarMap();	
		  PathExpr pe_eq = (PathExpr) ((PathExpr)recExpr()).clone(vm);
		  PathStep e = pe_eq.firstStep();
		  while (!(e instanceof PathReturn))
			  e = e.nextStep();

		  PathFieldValue right_most = new PathFieldValue(new ConstExpr(staticName()));
		  e.replaceInParent(right_most);
		  mt.add(pe, pe_eq, child_table.isSafeToMapAll());
	  }
	  else if (recExpr() instanceof IndexExpr)
	  {
		  //Create a PathExpr equivalent to this CopyField
		  VarMap vm = new VarMap();	
		  Expr left_child = recExpr().clone(vm);
		  PathFieldValue right_child = new PathFieldValue(new ConstExpr(staticName()));
		  PathExpr pe_eq = new PathExpr(left_child, right_child);
		  mt.add(pe, pe_eq, child_table.isSafeToMapAll());
	  }
	  else 
	  {
		  mt.add(pe, recExpr(), false);
	  }
	  return mt;
  }
  
  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#clone(com.ibm.jaql.lang.core.VarMap)
   */
  public Expr clone(VarMap varMap)
  {
    return cloneOrigin(new CopyField(cloneChildren(varMap), when));
  }

}
