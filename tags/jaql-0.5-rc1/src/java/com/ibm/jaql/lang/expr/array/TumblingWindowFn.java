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
import com.ibm.jaql.json.type.JsonEnum;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonUtil;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.type.MutableJsonLong;
import com.ibm.jaql.json.type.SpilledJsonArray;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.IterExpr;
import com.ibm.jaql.lang.expr.function.BuiltInFunctionDescriptor;
import com.ibm.jaql.lang.expr.function.Function;
import com.ibm.jaql.lang.expr.function.JsonValueParameter;
import com.ibm.jaql.lang.expr.function.JsonValueParameters;
import com.ibm.jaql.lang.expr.function.Parameter;
import com.ibm.jaql.lang.expr.function.Parameters;
import com.ibm.jaql.lang.util.JaqlUtil;

/**
 *  
 *
 */
public class TumblingWindowFn extends IterExpr
{
  private final static int ARG_INPUT = 0;
  private final static int ARG_STOP_PRED = 1;
  private final static int ARG_START_PRED = 2;
  private final static int ARG_FIRST_GROUP = 3;
  private final static int ARG_LAST_GROUP = 4;

  // stop arguments
  static enum StopArg implements JsonEnum
  {
    PREV("prev"),
    FIRST("first"),
    LAST("last"),
    NEXT("next"),
    SIZE("size");
    
    final JsonString argName;
    
    private StopArg(String name) 
    {
      this.argName = new JsonString(name);
    }

    @Override
    public JsonString jsonString() 
    {
      return argName; 
    }
  }

  // start arguments
  static enum StartArg implements JsonEnum
  {
    PREV("prev"),
    FIRST("first"),
    SIZE("size");
    
    final JsonString argName;
    
    private StartArg(String name) 
    {
      this.argName = new JsonString(name);
    }

    @Override
    public JsonString jsonString() 
    {
      return argName; 
    }
  }

  public static class Descriptor implements BuiltInFunctionDescriptor 
  {
    private Schema schema = new ArraySchema(null, SchemaFactory.arraySchema());
    private JsonValueParameters parameters;

    public Descriptor() {
      parameters = new JsonValueParameters(new JsonValueParameter[] {
          new JsonValueParameter("input", SchemaFactory.arrayOrNullSchema()),
          new JsonValueParameter("stop", SchemaFactory.functionSchema()),
          new JsonValueParameter("start", SchemaFactory.functionOrNullSchema(), null),
          new JsonValueParameter("firstGroup", SchemaFactory.booleanOrNullSchema(), JsonBool.TRUE),
          new JsonValueParameter("lastGroup", SchemaFactory.booleanOrNullSchema(), JsonBool.TRUE)
      });
    }

    @Override
    public Expr construct(Expr[] positionalArgs)
    {
      return new TumblingWindowFn(positionalArgs);
    }

    @Override
    public Class<? extends Expr> getImplementingClass() 
    {
      return TumblingWindowFn.class;
    }

    @Override
    public String getName()
    {
      return "tumblingWindow";
    }

    @Override
    public JsonValueParameters getParameters()
    {
      return parameters;
    }

    @Override
    public Schema getSchema()
    {
      return schema;
    }
  }

  
  public TumblingWindowFn(Expr... inputs)
  {
    super(inputs);
  }

  @Override
  public Schema getSchema()
  {
    Schema s = exprs[ARG_INPUT].getSchema().elements();
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
  public JsonIterator iter(final Context context) throws Exception
  {
    final JsonIterator iter = exprs[ARG_INPUT].iter(context);
    final Function stopfn = (Function)exprs[ARG_STOP_PRED].eval(context);
    final Function startfn = (Function)exprs[ARG_START_PRED].eval(context);
    final boolean firstGroup = JaqlUtil.ebv(exprs[ARG_FIRST_GROUP].eval(context));
    final boolean lastGroup = JaqlUtil.ebv(exprs[ARG_LAST_GROUP].eval(context));

    final CallNamedArgs stopPred = new CallNamedArgs(stopfn, StopArg.values(), false, context);
    final CallNamedArgs startPred =
      startfn == null ? null : new CallNamedArgs(startfn, StopArg.values(), false, context);

    if( ! iter.moveNext() )
    {
      return JsonIterator.EMPTY;
    }
    
    final MutableJsonLong windowSize = new MutableJsonLong();
    stopPred.setArg(StopArg.SIZE, windowSize);
    if( startPred != null )
    {
      startPred.setArg(StartArg.SIZE, windowSize);
    }

    // final JsonValue[] args = new JsonValue[] { null, null, null, null, windowSize };
    // Skip to the first break, if requested.
    if( !firstGroup )
    {
      stopPred.setArg(StopArg.NEXT, iter.current());
      windowSize.set(0);
      if( !JaqlUtil.ebv(stopPred.eval(context)) )
      {
        stopPred.setArgCopy(StopArg.FIRST, iter.current());
        long n = 0;
        do
        {
          stopPred.setArgCopy(StopArg.LAST, iter.current());
          if( ! iter.moveNext() )
          {
            return JsonIterator.EMPTY;
          }
          n++;
          stopPred.setArg(StopArg.NEXT, iter.current());
          windowSize.set(n);
        } while( !JaqlUtil.ebv(stopPred.eval(context)) );
      }
    }
    
    final SpilledJsonArray window = new SpilledJsonArray();
      
    return new JsonIterator(window) 
    {
      boolean hasNext = true;
      JsonValue last = null;

      public boolean moveNext() throws Exception
      {
        if( !hasNext )
        {
          return false;
        }
        
        if( startPred != null )
        {
          // find the start of the window
          long size = 0;
          startPred.setArg(StartArg.SIZE, windowSize);
          // prev = last
          startPred.setArg(StartArg.PREV, last);
          while( true )
          {
            startPred.setArg(StartArg.FIRST, iter.current());
            windowSize.set(size++);
            if( JaqlUtil.ebv(startPred.eval(context)) )
            {
              break;
            }
            last = JsonUtil.getCopy(iter.current(), last);
            startPred.setArg(StartArg.PREV, last);
            if( ! iter.moveNext() )
            {
              last = null;
              hasNext = false;
              return false;
            }
          }
        }

        // prev = last
        stopPred.setArgCopy(StopArg.PREV, last);

        // first = last = next
        // window = [last]
        last = JsonUtil.getCopy(iter.current(), last);
        window.clear();
        window.addCopy(last);
        stopPred.setArg(StopArg.FIRST, window.get(0));        
        
        // find the end of the window
        while( iter.moveNext() )
        {
          stopPred.setArg(StopArg.LAST, last);
          stopPred.setArg(StopArg.NEXT, iter.current());
          windowSize.set(window.count());
          if( JaqlUtil.ebv(stopPred.eval(context)) )
          {
            return true;
          }
          last = JsonUtil.getCopy(iter.current(), last);
          window.addCopy(last);
        }
        hasNext = false;
        return lastGroup;
      }
    };
  }
  
  public static class CallNamedArgs // TODO: move
  {
    protected Function fn;
    protected JsonEnum[] names;
    protected int[] index;
    protected JsonValue[] args;
    
    // expects names[i].ordinal() == i 
    public CallNamedArgs(Function fn, JsonEnum[] names, boolean strictNames, Context context) throws Exception
    {
      this.fn = fn;
      this.names = names;
      this.index = new int[names.length];
      Parameters<?> params = fn.getParameters();
      args = new JsonValue[params.numParameters()];
      boolean[] set = new boolean[args.length];
      for( JsonEnum name: names )
      {
        int p = params.positionOf(name.jsonString());
        index[name.ordinal()] = p;
        if( p >= 0 )
        {
          set[p] = true;
        }
        else if( strictNames )
        {
          throw new RuntimeException(fn.formatError("expected parameter "+name.jsonString()));
        }
      }
      // Set defaults for any unbound parameters.
      // Raise error if there is not default.
      for(int i = 0 ; i < args.length ; i++)
      {
        if( !set[i] )
        {
          Parameter<?> param = params.get(i);
          if( param.isOptional() )
          {
            //TODO: param.getDefaultValue(i, context);
            Object obj = param.getDefaultValue();
            if( obj instanceof Expr )
            {
              args[i] = ((Expr)obj).eval(context);
            }
            else
            {
              args[i] = (JsonValue)obj;
            }
          }
          else
          {
            throw new RuntimeException(fn.formatError("unexpected required parameter "+params.get(i).getName()));
          }
        }
      }
    }
    
    public void setArg(JsonEnum arg, JsonValue value)
    {
      int p = index[arg.ordinal()];
      if( p >= 0 )
      {
        args[p] = value;
      }
    }

    public void setArgCopy(JsonEnum arg, JsonValue value) throws Exception
    {
      int p = index[arg.ordinal()];
      if( p >= 0 )
      {
        args[p] = JsonUtil.getCopy(value, args[p]);
      }
    }


    public JsonValue eval(Context context) throws Exception
    {
      fn.setArguments(args);
      return fn.eval(context);
    }
  }
}
