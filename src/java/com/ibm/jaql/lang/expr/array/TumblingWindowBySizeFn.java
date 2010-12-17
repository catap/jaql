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
package com.ibm.jaql.lang.expr.array;

import com.ibm.jaql.json.schema.ArraySchema;
import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.type.JsonBool;
import com.ibm.jaql.json.type.JsonNumber;
import com.ibm.jaql.json.type.SpilledJsonArray;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.IterExpr;
import com.ibm.jaql.lang.expr.function.BuiltInFunctionDescriptor;
import com.ibm.jaql.lang.expr.function.JsonValueParameter;
import com.ibm.jaql.lang.expr.function.JsonValueParameters;
import com.ibm.jaql.lang.util.JaqlUtil;

/**
 *  
 *
 */
public class TumblingWindowBySizeFn extends IterExpr
{
  public final static int ARG_INPUT = 0;
  public final static int ARG_SIZE = 1;
  public final static int ARG_LAST_GROUP = 2;
  
  public static class Descriptor implements BuiltInFunctionDescriptor 
  {
    private JsonValueParameters parameters;

    public Descriptor() {
      parameters = new JsonValueParameters(new JsonValueParameter[] {
          new JsonValueParameter("input", SchemaFactory.arrayOrNullSchema()),
          new JsonValueParameter("size", SchemaFactory.numericSchema()),
          new JsonValueParameter("lastGroup", SchemaFactory.booleanOrNullSchema(), JsonBool.TRUE)
      });
    }

    @Override
    public Expr construct(Expr[] positionalArgs)
    {
      return new TumblingWindowBySizeFn(positionalArgs);
    }

    @Override
    public Class<? extends Expr> getImplementingClass() 
    {
      return TumblingWindowBySizeFn.class;
    }

    @Override
    public String getName()
    {
      return "tumblingWindowBySize";
    }

    @Override
    public JsonValueParameters getParameters()
    {
      return parameters;
    }

    @Override
    public Schema getSchema()
    {
      return SchemaFactory.arraySchema(); // TODO: refine
    }
  }

  
  public TumblingWindowBySizeFn(Expr... inputs)
  {
    super(inputs);
  }

  @Override
  public Schema getSchema()
  {
    Schema s = exprs[0].getSchema().elements();
    if( s == null )
    {
      return null;
    }
    s = new ArraySchema(null, s);
    s = new ArraySchema(null, s);
    return s;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.IterExpr#iter(com.ibm.jaql.lang.core.Context)
   */
  @Override
  protected JsonIterator iterRaw(final Context context) throws Exception
  {
    final JsonIterator iter = exprs[ARG_INPUT].iter(context);
    final long size = ((JsonNumber)exprs[ARG_SIZE].eval(context)).longValue();
    if( size <= 0 )
    {
      throw new RuntimeException("window size must be positive");
    }
    final boolean lastGroup = JaqlUtil.ebv(exprs[ARG_LAST_GROUP].eval(context));

    final SpilledJsonArray window = new SpilledJsonArray();
    // TODO: use faster simpler array when window is small?
    //final BufferedJsonArray window = new BufferedJsonArray();
      
    return new JsonIterator(window) 
    {
      boolean hasNext = true;
      
      protected boolean moveNextRaw() throws Exception
      {
        if( !hasNext )
        {
          return false;
        }
        window.clear();
        for(long i = 0 ; i < size ; i++)
        {
          if( ! iter.moveNext() )
          {
            hasNext = false;
            if( i > 0 && lastGroup )
            {
              return true;
            }
            return false;
          }
          window.addCopy(iter.current());
        }
        return true;
      }
    };
  }
}
