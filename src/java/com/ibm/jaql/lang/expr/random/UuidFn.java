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
package com.ibm.jaql.lang.expr.random;

import java.util.Map;
import java.util.UUID;

import com.ibm.jaql.json.schema.BinarySchema;
import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.type.JsonBinary;
import com.ibm.jaql.json.type.JsonLong;
import com.ibm.jaql.json.type.MutableJsonBinary;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.ExprProperty;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;

/**
 * @jaqlDescription Generate a type 4 UUID (random method)
 * Usage:
 * binary uuid()
 */
public class UuidFn extends Expr
{
  public final static Schema schema = new BinarySchema(new JsonLong(16));

  public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par00
  {
    public Descriptor()
    {
      super("uuid", UuidFn.class);
    }
  }
  
  private byte[] bytes = new byte[16];
  private MutableJsonBinary result = new MutableJsonBinary(bytes, false);

  /**
   * long randomLong(number seed)
   * 
   * @param exprs
   */
  public UuidFn(Expr... exprs)
  {
    super(exprs);
  }

  @Override
  public Map<ExprProperty, Boolean> getProperties()
  {
    Map<ExprProperty, Boolean> result = ExprProperty.createUnsafeDefaults();
    result.put(ExprProperty.IS_NONDETERMINISTIC, true);
    return result;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#eval(com.ibm.jaql.lang.core.Context)
   */
  protected JsonBinary evalRaw(final Context context) throws Exception
  {
    UUID uuid = UUID.randomUUID();
    long a = uuid.getMostSignificantBits();
    long b = uuid.getLeastSignificantBits();
    
    bytes[0]  = (byte)(a >> 56);
    bytes[1]  = (byte)(a >> 48);
    bytes[2]  = (byte)(a >> 40);
    bytes[3]  = (byte)(a >> 32);
    bytes[4]  = (byte)(a >> 24);
    bytes[5]  = (byte)(a >> 16);
    bytes[6]  = (byte)(a >>  8);
    bytes[7]  = (byte)(a      );
    
    bytes[8]  = (byte)(b >> 56);
    bytes[9]  = (byte)(b >> 48);
    bytes[10] = (byte)(b >> 40);
    bytes[11] = (byte)(b >> 32);
    bytes[12] = (byte)(b >> 24);
    bytes[13] = (byte)(b >> 16);
    bytes[14] = (byte)(b >>  8);
    bytes[15] = (byte)(b      );
    
    return result;
  }
  
  @Override
  public Schema getSchema()
  {
    return schema;
  }
}
