/*
 * Copyright (C) IBM Corp. 2010.
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

import java.util.Iterator;

import com.ibm.jaql.json.schema.ArraySchema;
import com.ibm.jaql.json.schema.RecordSchema;
import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.type.BufferedJsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonType;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.type.MutableJsonBool;
import com.ibm.jaql.json.type.MutableJsonSchema;
import com.ibm.jaql.json.type.MutableJsonString;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.core.Env;
import com.ibm.jaql.lang.core.Var;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;
import com.ibm.jaql.lang.expr.top.EnvExpr;

/**
 * List the active global variables.
 *   [{ var: string, schema: schema, isTable: boolean }...] 
 */
public final class ListVariablesFn extends IterExpr
{
  public final static JsonString VAR_NAME = new JsonString("var");
  public final static JsonString SCHEMA_NAME = new JsonString("schema");
  public final static JsonString ISTABLE_NAME = new JsonString("isTable");

  public final static ArraySchema schema = new ArraySchema(null,
      new RecordSchema(new RecordSchema.Field[]{
          new RecordSchema.Field(VAR_NAME, SchemaFactory.stringSchema(), false),
          new RecordSchema.Field(SCHEMA_NAME, SchemaFactory.schematypeSchema(), false),
          new RecordSchema.Field(ISTABLE_NAME, SchemaFactory.booleanSchema(), false),
      }, null));

  public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par00
  {
    public Descriptor()
    {
      super("listVariables", ListVariablesFn.class);
    }

    @Override
    public Schema getSchema()
    {
      return schema;
    }
  }

  public final static JsonString[] names = new JsonString[] {
      VAR_NAME,
      SCHEMA_NAME,
      ISTABLE_NAME
  };
  
  /**
   * BindingExpr inExpr, Expr projection
   * 
   * @param exprs
   */
  public ListVariablesFn(Expr[] exprs)
  {
    super(exprs);
  }

  @Override
  public Schema getSchema()
  {
    return schema;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.IterExpr#iter(com.ibm.jaql.lang.core.Context)
   */
  public JsonIterator iter(final Context context) throws Exception
  {
    EnvExpr root = getEnvExpr();
    if( root == null )
    {
      // TODO: this souldn't happen...
      return JsonIterator.NULL;
    }
    final Env env = root.getEnv();
    BufferedJsonRecord rec = new BufferedJsonRecord();
    final MutableJsonString varName = new MutableJsonString();
    final MutableJsonSchema varSchema = new MutableJsonSchema();
    final MutableJsonBool varIsTable = new MutableJsonBool();
    JsonValue[] values = new JsonValue[] {
        varName,
        varSchema,
        varIsTable
    };
    rec.set(names , values, names.length);

    return new JsonIterator(rec) 
    {
      Iterator<Var> vars = env.globals().listVariables(false).iterator();

      public boolean moveNext() throws Exception
      {
        if( vars.hasNext() )
        {
           Var v = vars.next();
           varName.setCopy(v.name());
           Schema schema = v.getSchema();
           varSchema.set(schema);
           varIsTable.set(false);
           if( schema.is(JsonType.ARRAY).always() )
           {
             Schema e = schema.elements();
             if( e != null && e.is(JsonType.RECORD).always() )
             {
               varIsTable.set(true);
             }
           }
           return true;
        }
        return false;
      }
    };
  }

}
