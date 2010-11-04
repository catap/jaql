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

import com.ibm.jaql.json.schema.ArraySchema;
import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.schema.SchemaTransformation;
import com.ibm.jaql.json.type.JsonBool;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.function.BuiltInFunctionDescriptor;
import com.ibm.jaql.lang.expr.function.Function;
import com.ibm.jaql.lang.expr.function.JsonValueParameter;
import com.ibm.jaql.lang.expr.function.JsonValueParameters;
import com.ibm.jaql.lang.util.JaqlUtil;

/**
 * Skip the first elements of input (in order) until the predicate is true,
 * return all elements after the test fires.
 * If inclusive is true (the default) then return the element that triggered
 * the condition.  Otherwise, exclude it. 
 * 
 * input: [T...]? -> skipUntil( when: fn(T): bool, inclusive:bool = true ): [T...]
 */
public class SkipUntilFn extends IterExpr
{
  public static class Descriptor implements BuiltInFunctionDescriptor 
  {
    private JsonValueParameters parameters;

    public Descriptor() {
      parameters = new JsonValueParameters(new JsonValueParameter[] {
          new JsonValueParameter("input", SchemaFactory.arrayOrNullSchema()),
          new JsonValueParameter("when", SchemaFactory.functionSchema()),
          new JsonValueParameter("inclusive", SchemaFactory.booleanOrNullSchema(), JsonBool.TRUE),
      });
    }

    @Override
    public Expr construct(Expr[] positionalArgs)
    {
      return new SkipUntilFn(positionalArgs);
    }

    @Override
    public Class<? extends Expr> getImplementingClass() 
    {
      return SkipUntilFn.class;
    }

    @Override
    public String getName()
    {
      return "skipUntil";
    }

    @Override
    public JsonValueParameters getParameters()
    {
      return parameters;
    }

    @Override
    public Schema getSchema()
    {
      return SchemaFactory.arraySchema();
    }
  }


  public SkipUntilFn(Expr... args)
  {
    super(args);
  }  
  
  @Override
  public Schema getSchema()
  {
    Schema s = exprs[0].getSchema();
    s = SchemaTransformation.restrictToArray(s);
    if( s == null )
    {
      return SchemaFactory.arraySchema();
    }
    return new ArraySchema(null,s.elements());
  }

  @Override
  public JsonIterator iter(final Context context) throws Exception
  {
    JsonValue[] args = new JsonValue[1];
    final JsonIterator iter = exprs[0].iter(context);
    Function pred = (Function)exprs[1].eval(context);
    while( true )
    {
      if( ! iter.moveNext() )
      {
        return JsonIterator.EMPTY;
      }
      
      args[0] = iter.current();
      pred.setArguments(args, 0, 1);
      JsonBool jb = (JsonBool)pred.eval(context);
      
      if( JaqlUtil.ebv(jb)  )
      {
        break;
      }
    }
    
    JsonBool incl = (JsonBool)exprs[2].eval(context);
    if( ! JaqlUtil.ebv(incl) )
    {
      // Not returning the signal element, so just return the input iterator.
      return iter;
    }

    // Need to return the signal element, but we've already pulled it from the iter, 
    // so make a new iterator to return the signal item plus the rest.
    return new JsonIterator(iter.current())
    {
      boolean atFirst = true;
      
      @Override
      public boolean moveNext() throws Exception
      {
        if( atFirst )
        {
          atFirst = false;
          return true;
        }
        if( iter.moveNext() )
        {
          currentValue = iter.current();
          return true;
        }
        return false;
      }
    };
  }
}
