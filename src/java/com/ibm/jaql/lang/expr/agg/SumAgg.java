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
package com.ibm.jaql.lang.expr.agg;

import java.math.BigDecimal;
import java.math.MathContext;
import java.util.ArrayList;
import java.util.List;

import com.ibm.jaql.json.schema.OrSchema;
import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.schema.SchemaTransformation;
import com.ibm.jaql.json.type.JsonDecimal;
import com.ibm.jaql.json.type.JsonDouble;
import com.ibm.jaql.json.type.JsonLong;
import com.ibm.jaql.json.type.JsonNumber;
import com.ibm.jaql.json.type.JsonType;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.JaqlFn;
import com.ibm.jaql.lang.expr.core.MathExpr;


/**
 * 
 */
@JaqlFn(fnName = "sum", minArgs = 1, maxArgs = 1)
public final class SumAgg extends AlgebraicAggregate
{
  public static final class Summer
  {
    boolean hadData;
    JsonType myType;
    private long lsum;
    private double dblSum;
    private BigDecimal decSum;

    public void init()
    {
      hadData = false;
      myType = JsonType.LONG;
      dblSum = lsum = 0;
      decSum = null;
    }
    
    public void add(JsonValue value)
    {
      if( value == null )
      {
        return;
      }
      if (value instanceof JsonNumber)
      {
        hadData = true;
        JsonNumber addend = (JsonNumber)value;
        
        // find out new type
        JsonType newType = MathExpr.promote(myType, addend.getType());
        if (newType != myType)
        {
          if (newType == JsonType.DOUBLE)
          {
            assert myType == JsonType.LONG;
            dblSum = lsum;
          }
          else
          {
            assert newType == JsonType.DECFLOAT;
            if (myType == JsonType.LONG)
            {
              decSum = new BigDecimal(lsum, MathContext.DECIMAL128);
            }
            else
            {
              decSum = new BigDecimal(dblSum, MathContext.DECIMAL128);
            }
          }
          myType = newType;
        }
        
        // add
        
        switch (myType)
        {
        case LONG:
          lsum += addend.longValue();
          break;
        case DOUBLE:
          dblSum += addend.doubleValue();
          break;
        case DECFLOAT:
          decSum = decSum.add(addend.decimalValue(), MathContext.DECIMAL128);
          break;
        }
      }
      else
      {
        throw new RuntimeException("Invalid input for sum aggregate: " + value);
      }
    }
    
    public JsonNumber get()
    {
      if (!hadData) return null;
      switch (myType)
      {
      case LONG:
        return new JsonLong(lsum);
      case DOUBLE:
        return new JsonDouble(dblSum);
      case DECFLOAT:
        return new JsonDecimal(decSum);
      }
      throw new IllegalStateException("cannot happen");
    }
    
    public static Schema getSchema(Schema in)
    {
      // compute schema in input elements
      Schema elements = SchemaTransformation.arrayElements(in);
      if (elements == null)
      {
        if (in.isEmpty(JsonType.ARRAY).maybe())
        {
          return SchemaFactory.nullSchema();
        }
        throw new RuntimeException("non-null array input expected");
      }
      
      // restrict schema to numeric types
      List<Schema> restricted = new ArrayList<Schema>(3);
      if (elements.is(JsonType.LONG).maybe())
      {
        restricted.add(SchemaFactory.longSchema());
      }
      if (elements.is(JsonType.DOUBLE).maybe())
      {
        restricted.add(SchemaFactory.doubleSchema());
      }
      if (elements.is(JsonType.DECFLOAT).maybe())
      {
        restricted.add(SchemaFactory.decfloatSchema());
      }
      
      // check validity
      if (restricted.size() == 0)
      {
        if (elements.is(JsonType.NULL).maybe())
        {
          return SchemaFactory.nullSchema();
        }
        throw new RuntimeException("array of numbers expected");
      }
     
      // construct schema
      restricted.add(SchemaFactory.nullSchema());
      return OrSchema.make(restricted);
    }
  }
  
  Summer summer = new Summer();
  
  /**
   * @param exprs
   */
  public SumAgg(Expr[] exprs)
  {
    super(exprs);
  }

  /**
   * @param expr
   */
  public SumAgg(Expr expr)
  {
    super(expr);
  }

  @Override
  public void initInitial(Context context) throws Exception
  {
    summer.init();
  }

  @Override
  public void addInitial(JsonValue value) throws Exception
  {
    summer.add(value);
  }

  @Override
  public JsonValue getPartial() throws Exception
  {
    return summer.get();
  }

  @Override
  public void addPartial(JsonValue value) throws Exception
  {
    summer.add(value);
  }

  @Override
  public JsonValue getFinal() throws Exception
  {
    return summer.get();
  }


  @Override
  public Schema getPartialSchema()
  {
    return getSchema();
  }

  @Override
  public Schema getSchema()
  {
    Schema in = exprs[0].getSchema();
    return Summer.getSchema(in);
  }
}

