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
package com.ibm.jaql.lang.expr.io;

import com.ibm.jaql.io.Adapter;
import com.ibm.jaql.io.stream.StreamInputAdapter;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.lang.core.Env;
import com.ibm.jaql.lang.expr.core.ConstExpr;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.MacroExpr;
import com.ibm.jaql.lang.expr.core.NameValueBinding;
import com.ibm.jaql.lang.expr.core.RecordExpr;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;
import com.ibm.jaql.lang.expr.record.RemapFn;

/**
 * 
 */
public class HttpGetExpr extends MacroExpr
{
  public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par13
  {
    public Descriptor()
    {
      super("httpGet", HttpGetExpr.class);
    }
  }
  
  /**
   * @param exprs
   */
  public HttpGetExpr(Expr[] exprs)
  {
    super(exprs);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.MacroExpr#expand(com.ibm.jaql.lang.core.Env)
   */
  @Override
  public Expr expandRaw(Env env) throws Exception
  {
    NameValueBinding tField = new NameValueBinding(Adapter.TYPE_NAME,
        new ConstExpr(new JsonString("uri")));
    NameValueBinding lField = new NameValueBinding(Adapter.LOCATION_NAME,
        exprs[0]);
    RecordExpr rec;
    if (exprs.length > 1)
    {
      NameValueBinding aField = new NameValueBinding(
          StreamInputAdapter.ARGS_NAME, exprs[1]);
      
      RecordExpr oRec = new RecordExpr(new Expr[]{aField});
      Expr oExpr = oRec;
      if(exprs.length > 2) 
      {
        oExpr = new RemapFn(oRec, exprs[2]);
      } 
      NameValueBinding oField = new NameValueBinding(Adapter.INOPTIONS_NAME,
          oExpr);
      rec = new RecordExpr(new Expr[]{tField, lField, oField});
    }
    else
    {
      rec = new RecordExpr(new Expr[]{tField, lField});
    }
    return new ReadFn(rec);
  }
}
