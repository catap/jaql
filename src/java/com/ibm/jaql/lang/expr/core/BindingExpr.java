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

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.ibm.jaql.json.schema.ArraySchema;
import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.schema.SchemaTransformation;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.core.VarMap;
import com.ibm.jaql.lang.expr.metadata.MappingTable;
import com.ibm.jaql.util.Bool3;
import com.ibm.jaql.util.FastPrinter;

/**
 * A BindingExpr is not really an Expr at all. It is used to associate a
 * variable with its definition (or one of its definitions in the case of
 * GROUP_BY). It should never be evaluated or decompiled because the expressions
 * that use bindings know how to walk over these guys.
 */
public class BindingExpr extends Expr
{
  public static enum Type
  {
    EQ(), IN(), INREC(), INPAIR(), INAGG(), AGGFN(),
  }

  public Type type;

  //           exprs[0]        exprs[1]
  //    EQ     defining expr   n/a
  //    IN     array expr      the on-clause of "join"
  //    INREC  record expr     n/a

  // var is always required
  //    EQ     the variable being defined by "let", or by-clause of "group"
  //    IN     the iteration variable defined by "for", "join", or in-clause of "group"
  //    INREC  the name variable
  public Var     var;             // FIXME: make protected

  // var2
  //    EQ     null
  //    IN     optional "at" index for "for"
  //           required "into" variable for "group"
  //    INREC  required value variable
  public Var     var2;            // FIXME: make protected

  /**
   * For a join expression, true means this input is preserved in the output (no records dropped).
   */
  public boolean preserve = false; // FIXME: make protected

  /**
   * @param type
   * @param var
   * @param var2
   * @param optional
   * @param exprs
   */
  public BindingExpr(Type type, Var var, Var var2, boolean preserved, Expr[] exprs)
  {
    super(exprs);
    this.type = type;
    this.var = var;
    this.var2 = var2;
    this.preserve = preserved;
  }

  /**
   * 
   * @param type
   * @param var
   * @param var2
   * @param preserved
   * @param expr
   */
  public BindingExpr(Type type, Var var, Var var2, boolean preserved, Expr expr)
  {
    super(expr);
    this.type = type;
    this.var = var;
    this.var2 = var2;
    this.preserve = preserved;
  }

  /**
   * @param type
   * @param var
   * @param var2
   * @param exprs
   */
  public BindingExpr(Type type, Var var, Var var2, Expr[] exprs)
  {
    this(type, var, var2, false, exprs);
  }

  /**
   * @param type
   * @param var
   * @param var2
   * @param expr
   */
  public BindingExpr(Type type, Var var, Var var2, Expr expr)
  {
    this(type, var, var2, false, new Expr[]{expr});
  }

  /**
   * @param type
   * @param var
   * @param var2
   * @param expr0
   * @param expr1
   */
  public BindingExpr(Type type, Var var, Var var2, Expr expr0, Expr expr1)
  {
    this(type, var, var2, false, new Expr[]{expr0, expr1});
  }

  public Bool3 getProperty(ExprProperty prop, boolean deep)
  {
    Map<ExprProperty, Boolean> props = getProperties();
    if (deep)
    {
      return getProperty(props, prop, exprs);
    }
    else
    {
      return getProperty(props, prop, null);
    }    
  }
  
  
  /**
   * 
   */
  @Override
  public Bool3 evaluatesChildOnce(int i)
  {
    return Bool3.TRUE;
  }


  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#decompile(java.io.PrintStream,
   *      java.util.HashSet)
   */
  protected void decompileRaw(FastPrinter exprText, HashSet<Var> capturedVars, boolean emitLocation)
      throws Exception
  {
    exprText.print(var.taggedName());
    exprText.print(" = ");
    if( exprs.length == 0 )
    {
      exprText.print("??");
    }
    else
    {
      exprs[0].decompile(exprText, capturedVars,emitLocation);
    }
    // throw new RuntimeException("BindingExpr should never be decompiled");
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#eval(com.ibm.jaql.lang.core.Context)
   */
  @Override
protected JsonValue evalRaw(Context context) throws Exception
  {
    //throw new RuntimeException("BindingExpr should never be evaluated");
    var.setEval(exprs[0], context); // TODO: set var.usage
    return null;
  }

  /**
   * Returns iter that returns the input plus a side-effect of setting the variable
   * to each element. 
   */
  @Override
  protected JsonIterator iterRaw(final Context context) throws Exception
  {
    //throw new RuntimeException("BindingExpr should never be evaluated");
    var.undefine();
    final JsonIterator iter = exprs[0].iter(context);
    return new JsonIterator()
    {
      @Override
	protected boolean moveNextRaw() throws Exception
      {
        if (iter.moveNext()) {
          currentValue = iter.current();
          var.setValue(currentValue);
          return true;
        } 
        var.undefine();
        return false;
      }
    };
  }
  

  
  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#clone(com.ibm.jaql.lang.core.VarMap)
   */
  public Expr clone(VarMap varMap)
  {
    return cloneOrigin(new BindingExpr(type, varMap.remap(var), varMap.remap(var2),
        preserve, cloneChildren(varMap)));
  }

  /**
   * @return
   */
  public final Expr eqExpr()
  {
    return exprs[0];
  }

  /**
   * @return
   */
  public final Expr inExpr()
  {
    return exprs[0];
  }

//  /**
//   * @return
//   */
//  public final Expr onExpr()
//  {
//    return exprs[1];
//  }
//
  /**
   * @param i
   * @return
   */
  public final Expr byExpr(int i)
  {
    return exprs[i];
  }

  
  /**
   * Return one mapping table that merges the mapping table from all children.
   */
  @Override
  public MappingTable getMappingTable()
  {
	  MappingTable mt = new MappingTable();

      for (int i = 0; i < exprs.length; i++)
    	  mt.addAll(exprs[i].getMappingTable());
      return mt;
  }
  
  
  /**
   * Return the mapping table for child "child_id".
   */
  @Override
  public MappingTable getMappingTable(int child_id)
  {
	  assert (child_id < exprs.length);
	  return (exprs[child_id].getMappingTable());
  }
   

  /** 
   * Returns an array of schemata that are bound to the variable, respecting order, and sets
   * the schema associated with the variable to a schema representing all possible bindings.
   */
  public Schema getSchema()
  {
    if (exprs.length == 0)
    {
      return var.getSchema();
    } 
    
    switch (type)
    {
    case EQ: // array of all inputs
      List<Schema> schemata = new LinkedList<Schema>();
      for (int i=0; i<exprs.length; i++)
      {
        Schema s = exprs[i].getSchema();
        assert s != null;
        schemata.add(s);
      }
      Schema r = new ArraySchema(schemata);
      Schema re = r.elements();
      if (re != null)
      {
        // array is non-empty
        var.setSchema(re); // update the schema associated with the variable
      }
      else
      {
        // variable will never be set
      }
      return r;
    case IN: // array of all elements in the inputs
      Schema varSchema = null; 
      for (int i=0; i<exprs.length; i++)
      {
        Schema inputSchema = exprs[i].getSchema();
        Schema inputSchemaArray = SchemaTransformation.restrictToArrayOrNull(inputSchema);
        if (inputSchemaArray == null) {
          throw new IllegalArgumentException("array expected"); 
        }
        if (inputSchemaArray.is(NULL).always())
        {
            continue; 
        }

        // schema now is "array" or "array?"
        Schema inputElements = inputSchemaArray.elements();
        if (inputElements != null)   
        {
          // update schema information
          if (varSchema == null)
          {
            varSchema = inputElements;
          }
          else
          {
            varSchema = SchemaTransformation.merge(varSchema, inputElements);
          }
        }
      }
      if (varSchema != null) 
      {
        var.setSchema(varSchema); // update the schema associated with the variable
        return new ArraySchema(null, varSchema);
      }
      else
      {
        return SchemaFactory.emptyArraySchema();
      }      
    }
    // TODO implement other types

    return SchemaFactory.anySchema();
  }
  
}
