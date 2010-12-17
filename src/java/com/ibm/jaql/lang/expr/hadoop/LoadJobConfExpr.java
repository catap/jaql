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
package com.ibm.jaql.lang.expr.hadoop;

import java.io.File;
import java.io.IOException;
import java.util.EnumMap;
import java.util.Map;

import org.apache.hadoop.mapred.JobConf;

import com.ibm.jaql.json.schema.RecordSchema;
import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.type.BufferedJsonRecord;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonType;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.ExprProperty;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;
import com.ibm.jaql.lang.util.JaqlUtil;

/**
 * @jaqlDescription load a Hadoop JobConf into a record
 * Usage: 
 * { *: string, * } loadJobConf( string? filename )
 * 
 * If filename to conf is not specified, then the default JobConf is loaded.
 */
public class LoadJobConfExpr extends Expr
{
  public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par01
  {
    public Descriptor()
    {
      super("loadJobConf", LoadJobConfExpr.class);
    }
  }
  
  private static final Map<ExprProperty, Boolean> properties = new EnumMap<ExprProperty, Boolean>(ExprProperty.class);
  static {
    properties.put(ExprProperty.ALLOW_COMPILE_TIME_COMPUTATION, true);
    properties.put(ExprProperty.HAS_CAPTURES, false);
    properties.put(ExprProperty.HAS_SIDE_EFFECTS, false);
    properties.put(ExprProperty.IS_NONDETERMINISTIC, false);
    properties.put(ExprProperty.READS_EXTERNAL_DATA, true);
  };
  
  /**
   * @param exprs
   */
  public LoadJobConfExpr(Expr[] exprs)
  {
    super(exprs);
  }
  
  /* (non-Javadoc)
   * @see com.ibm.jaql.lang.expr.core.Expr#getProperties()
   */
  @Override
  public Map<ExprProperty, Boolean> getProperties()
  {
    return properties;
  }
  
  /* (non-Javadoc)
   * @see com.ibm.jaql.lang.expr.core.Expr#getSchema()
   */
  @Override
  public Schema getSchema() 
  {
    Schema s = null;
    try {
      s = SchemaFactory.parse("{ *:string }");
    } catch(IOException e) {
      JaqlUtil.rethrow(e);
    }
    return s;
  }
  
  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#eval(com.ibm.jaql.lang.core.Context)
   */
  @Override
  protected JsonValue evalRaw(Context context) throws Exception
  {
    JobConf conf;
    if(exprs.length == 0) {
      conf = new JobConf();
    } else {
      JsonValue v = exprs[0].eval(context);
      String path = ((JsonString)v).toString();
      conf = new JobConf(path);
    }
       
    BufferedJsonRecord rec = new BufferedJsonRecord();
    
    for(Map.Entry<String, String> e : conf) {
      rec.add(new JsonString(e.getKey()), new JsonString(e.getValue()));
    }
    
    return rec;
  }
}