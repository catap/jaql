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
package com.ibm.jaql.lang.expr.schema;

import java.util.List;

import com.ibm.jaql.json.schema.RecordSchema;
import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaTransformation;
import com.ibm.jaql.json.schema.RecordSchema.Field;
import com.ibm.jaql.json.type.BufferedJsonRecord;
import com.ibm.jaql.json.type.JsonSchema;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonType;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.type.MutableJsonBool;
import com.ibm.jaql.json.type.MutableJsonLong;
import com.ibm.jaql.json.type.MutableJsonSchema;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.IterExpr;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;
import com.ibm.jaql.util.Bool3;

/**
 * elementsOf(schema): if schema is (potentially) an record schema, return a table describing its known fields:
 *     [{ name: string, schema: schema, index: long }...]
 */
public class FieldsOfFn extends IterExpr
{
  public final static JsonString[] names = new JsonString[] {
    new JsonString("name"),
    new JsonString("schema"),
    new JsonString("isOptional"),
    new JsonString("index")
  };

  public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par11
  {
    public Descriptor()
    {
      super("fieldsOf", FieldsOfFn.class);
    }
  }
  
  /**
   * @param exprs
   */
  public FieldsOfFn(Expr... exprs)
  {
    super(exprs);
  }


  /**
   * schemaof(e) never evaluates e
   */
  @Override
  public Bool3 evaluatesChildOnce(int i)
  {
    return Bool3.TRUE;
  }

  protected JsonIterator iterRaw(final Context context) throws Exception
  {
    JsonSchema jschema = (JsonSchema)exprs[0].eval(context);
    if( jschema == null )
    {
      return JsonIterator.NULL;
    }
    Schema schema = SchemaTransformation.restrictTo(jschema.get(), JsonType.RECORD);
    if( schema == null )
    {
      return JsonIterator.NULL;
    }
    final RecordSchema recSchema = (RecordSchema)schema;
    final List<Field> fields = recSchema.getFieldsByPosition();
    BufferedJsonRecord rec = new BufferedJsonRecord();
    final MutableJsonSchema fschema = new MutableJsonSchema();
    final MutableJsonBool foptional = new MutableJsonBool();
    final MutableJsonLong findex = new MutableJsonLong();
    final JsonValue[] values = new JsonValue[] {
        null, // set later
        fschema,
        foptional,
        findex
    };
    rec.set(names, values, names.length);

    return new JsonIterator(rec)
    {
      int i = 0;
      
      @Override
      protected boolean moveNextRaw() throws Exception
      {
        if( i == fields.size() )
        {
          return false;
        }
        Field f = fields.get(i);
        values[0] = f.getName();
        fschema.set(f.getSchema());
        foptional.set(f.isOptional());
        findex.set(i);
        i++;
        return true;
      }
    };
  }
}
