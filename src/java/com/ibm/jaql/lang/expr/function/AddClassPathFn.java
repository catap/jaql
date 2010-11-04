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
package com.ibm.jaql.lang.expr.function;

import java.io.File;

import com.ibm.jaql.json.type.JsonArray;
import com.ibm.jaql.json.type.JsonBool;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.util.ClassLoaderMgr;


/** Add jars to the classpath */
public class AddClassPathFn extends Expr
{
	public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par11
	{
	  public Descriptor()
	  {
	    super("addClassPath", AddClassPathFn.class);
	  }	   
	}
  
  public AddClassPathFn(Expr ... exprs)
  {
    super(exprs);
  }

  @Override
  public JsonValue eval(Context context) throws Exception 
  {
    // TODO: make this a MacroExpr to load classes immediately? Otherwise explain of javaudf will not work...
    JsonArray jarray = (JsonArray)exprs[0].eval(context);
    int n = (int)jarray.count();
    File[] files = new File[n];
    for(int i = 0 ; i < n ; i++)
    {
      JsonString jfile = (JsonString)jarray.get(i);
      files[i] = new File(jfile.toString());
    }
    ClassLoaderMgr.addExtensionJars(files);
	  return JsonBool.TRUE;
  }
}