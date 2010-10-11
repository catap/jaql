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
import java.util.Map.Entry;

import com.ibm.jaql.json.schema.OrSchema;
import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.type.BufferedJsonRecord;
import com.ibm.jaql.json.type.JsonArray;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.type.SpilledJsonArray;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;

/**
 * DEPRECATED: is this used anywhere?
 * Merge a set of arrays into one array in order, or a set of records into one record.  Nulls are ignored.
 *
 */
public class MergeContainersFn extends Expr
{
  public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par1u
  {
    public Descriptor()
    {
      super("mergeContainers", MergeContainersFn.class);
    }
  }
  
  public MergeContainersFn(Expr[] inputs)
  {
    super(inputs);
  }
  
  public MergeContainersFn(ArrayList<Expr> inputs)
  {
    super(inputs);
  }

  public MergeContainersFn(Expr expr0, Expr expr1)
  {
    super(expr0, expr1);
  }
  
  public Schema getSchema()
  {
    // TODO: refine
    return OrSchema.make(
        SchemaFactory.nullSchema(), 
        SchemaFactory.arraySchema(), 
        SchemaFactory.recordSchema());
  }

  @Override
  public JsonValue eval(Context context) throws Exception
  {
    int i;
    JsonValue value = null;
    for( i = 0 ; i < exprs.length ; i++ )
    {
      value = exprs[i].eval(context);
      if( value != null  )
      {
        break;
      }
    }
    JsonValue result;
    if( value == null )
    {
      result = null;
    }
    else if( value instanceof JsonArray )
    {
      SpilledJsonArray resultArr = new SpilledJsonArray(); // TODO: memory
      while(true)
      {
        if( value != null )
        {
          JsonArray arr = (JsonArray)value;
          resultArr.addCopyAll(arr.iter());
        }
        i++;
        if( i >= exprs.length )
        {
          break;
        }
        value = exprs[i].eval(context);
      }
      result = resultArr;
    }
    else if( value instanceof JsonRecord )
    {
      BufferedJsonRecord resultRec = new BufferedJsonRecord(); // TODO: memory
      while(true)
      {
        if( value != null )
        {
          JsonRecord rec = (JsonRecord)value;
          for (Entry<JsonString, JsonValue> e : rec)
          {
            resultRec.add(e.getKey(), e.getValue());
          }
        }
        i++;
        if( i >= exprs.length )
        {
          break;
        }
        value = exprs[i].eval(context);
      }
      result = resultRec; // TODO: memory
    }
    else
    {
      throw new RuntimeException("mergeContainers() can only merge all arrays or all records");
    }
    
    return result;
  }

  @Override
  public JsonIterator iter(final Context context) throws Exception
  {
    return new JsonIterator()
    {
      int input = 0;
      JsonIterator iter = JsonIterator.EMPTY;
      
      @Override
      public boolean moveNext() throws Exception
      {
        while( true )
        {
          if (iter.moveNext()) {
            currentValue = iter.current();
            return true;
          }
          if( input >= exprs.length )
          {
            return false;
          }
          iter = exprs[input++].iter(context);
        }
      }
    };
  }
}
