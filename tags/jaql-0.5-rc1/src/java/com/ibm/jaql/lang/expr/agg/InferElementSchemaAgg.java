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
package com.ibm.jaql.lang.expr.agg;

import java.util.Iterator;
import java.util.Map.Entry;

import com.ibm.jaql.json.schema.ArraySchema;
import com.ibm.jaql.json.schema.RecordSchema;
import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.schema.SchemaTransformation;
import com.ibm.jaql.json.schema.RecordSchema.Field;
import com.ibm.jaql.json.type.JsonArray;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonSchema;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;


/**
 * Infer a schema that describes all of the elements.
 */
public final class InferElementSchemaAgg extends AlgebraicAggregate
{
  public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par11
  {
    public Descriptor()
    {
      super("inferElementSchema", InferElementSchemaAgg.class);
    }
  }
  
  protected Schema schema;
  
  
  /**
   * @param exprs
   */
  public InferElementSchemaAgg(Expr[] exprs)
  {
    super(exprs);
  }

  /**
   * @param expr
   */
  public InferElementSchemaAgg(Expr expr)
  {
    super(expr);
  }

  @Override
  public void init(Context context) throws Exception
  {
  }
  
  public static Schema inferSchema(JsonValue value) throws Exception
  {
    Schema result;
    switch( value.getEncoding().getType() )
    {
      case ARRAY: {
        Schema head[] = null; 
        Schema rest = null;
        JsonArray arr = (JsonArray)value;
        long c = arr.count();
        JsonIterator iter = arr.iter();
        if( c <= 100 ) // TODO: Infer head of the array?  How much?
        {
          int n = (int)c;
          head = new Schema[n];
          for(int i = 0 ; i < n ; i++)
          {
            iter.moveNext();
            head[i] = inferSchema(iter.current());
          }
        }
        if( iter.moveNext() )
        {
          rest = inferSchema(iter.current());
          while( iter.moveNext() )
          {
            Schema s = inferSchema(iter.current());
            rest = SchemaTransformation.merge(rest, s);
          }
        }
        result = new ArraySchema(head, rest);
        break;
      }
      case RECORD: {
        JsonRecord rec = (JsonRecord)value;
        int n = rec.size();
        Field[] fields = new Field[n];
        Iterator<Entry<JsonString, JsonValue>> iter = rec.iterator();
        for( int i = 0 ; i < n ; i++ )
        {
          Entry<JsonString, JsonValue> f = iter.next();
          Schema s = inferSchema(f.getValue());
          fields[i] = new Field(f.getKey(), s, false);
        }
        result = new RecordSchema(fields, null);
        break;
      }
      default:
        result = SchemaFactory.make(value.getEncoding().getType());
    }
    return result;
  }

  @Override
  public void accumulate(JsonValue value) throws Exception
  {
    if( schema == null )
    {
      schema = inferSchema(value);
    }
    else if( ! schema.matchesUnsafe(value) )
    {
      combine(inferSchema(value));
    }
  }

  @Override
  public JsonSchema getPartial() throws Exception
  {
    if( schema == null )
    {
      return null;
    }
    return new JsonSchema(schema);
  }

  /**
   * Merge this schema into the current schema
   */
  public void combine(Schema s)
  {
    if( schema == null )
    {
      schema = s;
    }
    else
    {
      schema = SchemaTransformation.merge(schema, s);
    }
  }
  
  @Override
  public void combine(JsonValue value) throws Exception
  {
    Schema s = ((JsonSchema)value).get();
    combine(s);
  }

  @Override
  public JsonSchema getFinal() throws Exception
  {
    return getPartial();
  }

  @Override
  public Schema getPartialSchema()
  {
    return SchemaFactory.schematypeOrNullSchema();
  }

  @Override
  public Schema getSchema()
  {
    return SchemaFactory.schematypeOrNullSchema();
  }
}
