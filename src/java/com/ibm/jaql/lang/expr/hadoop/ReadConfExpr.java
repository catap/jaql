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

import org.apache.hadoop.mapred.JobConf;

import com.ibm.jaql.io.hadoop.Globals;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;

/**
 * @jaqlDescption read a value from jaql's current Hadoop JobConf
 * Usage:
 * string readConf(string name, string? dflt);
 * 
 * Jaql stores the JobConf that is associated with the current
 * map-reduce job. This function reads name from this JobConf and
 * returns its value, otherwise it returns the dflt value.
 * 
 */
public class ReadConfExpr extends Expr
{
  public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par12
  {
    public Descriptor()
    {
      super("readConf", ReadConfExpr.class);
    }
  }
  
  /**
   * @param exprs
   */
  public ReadConfExpr(Expr[] exprs)
  {
    super(exprs);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#eval(com.ibm.jaql.lang.core.Context)
   */
  @Override
  public JsonValue eval(Context context) throws Exception
  {
    JobConf conf = Globals.getJobConf();
    if( conf == null ) {
    	conf = new JobConf();
    	Globals.setJobConf(conf);
    }

    JsonString name = (JsonString) exprs[0].eval(context);
    String val = conf.get(name.toString());
    JsonValue dflt = null;
    if (val == null)
    {
      dflt = exprs[1].eval(context);
      return dflt;
    }

    return new JsonString(val);
  }
}
