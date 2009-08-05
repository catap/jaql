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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.ibm.jaql.json.schema.RecordSchema;
import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.schema.SchemaTransformation;
import com.ibm.jaql.json.type.BufferedJsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.Env;
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.expr.nil.NullElementOnEmptyFn;
import com.ibm.jaql.util.Bool3;

//TODO: add optimized RecordExpr when all cols are known at compile time
/**
 * 
 */
public class RecordExpr extends Expr
{
  protected BufferedJsonRecord record;

  /**
   * every exprs[i] is a FieldExpr
   * 
   * @param exprs
   */
  public RecordExpr(Expr ... exprs)
  {
    super(exprs);
  }

  /**
   * 
   */
  public RecordExpr()
  {
    super();
  }
  
  /**
  /**
   * 
   * @param fields
   */
  public RecordExpr(ArrayList<FieldExpr> fields)
  {
    super(fields);
  }

  /**
   * Either construct a RecordExpr or some for loops over a RecordExpr
   * to handle flatten requests.
   * 
   * @param args
   * @return
   */
  public static Expr make(Env env, Expr[] args)
  {
    int n = 0;
    for(Expr e: args)
    {
      if( e instanceof NameValueBinding &&
          e.child(1) instanceof FlattenExpr )
      {
        n++;
      }
    }
    if( n == 0 )
    {
      return new RecordExpr(args);
    }

    Var[] vars = new Var[n];
    Expr[] ins = new Expr[n];
    Expr[] doargs = new Expr[n+1];
    n = 0;
    for(int i = 0 ; i < args.length ; i++)
    {
      if( args[i] instanceof NameValueBinding && 
          args[i].child(1) instanceof FlattenExpr )
      {
        Expr flatten = args[i].child(1);
        Var letVar = new Var("$_toflat_"+n);
        vars[n] = env.makeVar("$_flat_"+n);
        ins[n] = new VarExpr(letVar);
        Expr e = flatten.child(0);
        if( e.getSchema().isEmptyArrayOrNull().maybe() )
        {
          e = new NullElementOnEmptyFn(e);
        }
        doargs[n] = new BindingExpr(BindingExpr.Type.EQ, letVar, null, e);
        flatten.replaceInParent(new VarExpr(vars[n]));
        n++;
      }
    }
    Expr e = new ArrayExpr(new RecordExpr(args));
    for( n-- ; n >= 0 ; n-- )
    {
      e = new ForExpr(vars[n], ins[n], e);
    }
    doargs[doargs.length-1] = e;
    e = new DoExpr(doargs);
    return e;
  }

  @Override
  public Schema getSchema()
  {
    List<RecordSchema.Field> fields = new LinkedList<RecordSchema.Field>();
    Schema unresolved = null; // schema of all fields that could not be matched

    for (Expr e : exprs)
    {
      FieldExpr fe = (FieldExpr)e;
      if (fe instanceof NameValueBinding) // name: value
      {
        NameValueBinding ne = (NameValueBinding)fe;
        JsonString name = ne.staticName();
        if (name != null)
        {
          fields.add(new RecordSchema.Field(name, ne.valueExpr().getSchema(), !ne.required));
        }
        else
        {
          if (unresolved == null)
          {
            unresolved = ne.valueExpr().getSchema();
          }
          else
          {
            unresolved = SchemaTransformation.merge(unresolved, ne.valueExpr().getSchema());
          }          
        }
      } 
      else if (fe instanceof CopyField) // $.a
      {
        CopyField ce = (CopyField)fe;
        JsonString name = ce.staticName();
        Schema recordSchema = ce.recExpr().getSchema();
        if (name != null)
        {
          Bool3 hasElement = recordSchema.hasElement(name);
          if (hasElement.maybe())
          {
            Schema valueSchema = recordSchema.element(name);
            if (valueSchema==null) valueSchema = SchemaFactory.anySchema(); // don't know better
            fields.add(new RecordSchema.Field(name, valueSchema, hasElement.always() ? false : true));
          }
        }
        else
        {
          if (unresolved == null)
          {
            unresolved = ce.recExpr().getSchema().elements();
          }
          else
          {
            unresolved = SchemaTransformation.merge(unresolved, ce.recExpr().getSchema().elements());
          }
        }
      }
      else if (fe instanceof CopyRecord) // $.*
      {
        CopyRecord ce = (CopyRecord)fe;
        Schema copySchema = ce.exprs[0].getSchema();
        if (copySchema instanceof RecordSchema)
        {
          for (RecordSchema.Field f : ((RecordSchema)copySchema).getFields())
          {
            fields.add(f);
          }
          Schema rest = ((RecordSchema)copySchema).getRest();
          if (rest!=null)
          {
            unresolved = unresolved==null ? rest : SchemaTransformation.merge(unresolved, rest);
          }
        }
        else
        {
          unresolved = SchemaFactory.anySchema();
        }
      }
      else // unknown FieldExpr
      {
        unresolved = SchemaFactory.anySchema();
      }
    }
    
    RecordSchema.Field[] fieldsArray = fields.toArray(new RecordSchema.Field[fields.size()]);
    return new RecordSchema(fieldsArray, unresolved);
  }

  public Map<ExprProperty, Boolean> getProperties() 
  {
    Map<ExprProperty, Boolean> result = super.getProperties();
    result.put(ExprProperty.ALLOW_COMPILE_TIME_COMPUTATION, true);
    return result;
  }

  /**
   * 
   */
  @Override
  public Bool3 evaluatesChildOnce(int i)
  {
    return Bool3.TRUE;
  }

  /**
   * @return
   */
  public final int numFields()
  {
    return exprs.length;
  }

  /**
   * @param i
   * @return
   */
  public final FieldExpr field(int i)
  {
    return (FieldExpr) exprs[i];
  }

  /**
   * Return the value expression of the field with the given name, if it can be
   * found at compile time. A null means the field was not found at
   * compile-time, but it could still exist at run-time.
   * 
   * @param name
   * @return
   */
  public Expr findStaticFieldValue(JsonString name)
  {
    Expr valExpr = null;
    for (int i = 0; i < exprs.length; i++)
    {
      if (exprs[i] instanceof NameValueBinding)
      {
        NameValueBinding nv = (NameValueBinding) exprs[i];
        Expr nameExpr = nv.nameExpr();
        if (nameExpr instanceof ConstExpr)
        {
          ConstExpr ce = (ConstExpr) nameExpr;
          JsonValue t = ce.value;
          if (t instanceof JsonString)
          {
            if (name.equals(t))
            {
              if (valExpr != null)
              {
                throw new RuntimeException(
                    "duplicate field name in record constructor: " + name);
              }
              valExpr = nv.valueExpr();
            }
          }
        }
      }
    }
    return valExpr;
  }

  /**
   * @param name
   * @return
   */
  public Expr findStaticFieldValue(String name) // TODO: migrate callers to JString version
  {
    return findStaticFieldValue(new JsonString(name)); // TODO: memory
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
    exprText.print("{");
    String sep = " ";
    for (int i = 0; i < exprs.length; i++)
    {
      exprText.print(sep);
      exprs[i].decompile(exprText, capturedVars);
      sep = ", ";
    }
    exprText.print(" }");
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#eval(com.ibm.jaql.lang.core.Context)
   */
  public JsonValue eval(Context context) throws Exception
  {
    if (record == null)
    {
      record = new BufferedJsonRecord();
    }
    else
    {
      record.clear();
    }

    for (int i = 0; i < exprs.length; i++)
    {
      FieldExpr f = (FieldExpr) exprs[i];
      f.eval(context, record);
    }
    return record;
  }
}
