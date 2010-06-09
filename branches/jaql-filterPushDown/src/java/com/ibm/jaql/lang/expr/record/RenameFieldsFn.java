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
package com.ibm.jaql.lang.expr.record;

import java.util.Map.Entry;

import com.ibm.jaql.json.type.BufferedJsonRecord;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;

/**
 * 
 */
public class RenameFieldsFn extends Expr
{
  public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par22
  {
    public Descriptor()
    {
      super("renameFields", RenameFieldsFn.class);
    }
  }
  
  /**
   * renameField(rec, [[oldName, newName]] )
   * 
   * @param exprs
   */
  public RenameFieldsFn(Expr[] exprs)
  {
    super(exprs);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.Expr#eval(com.ibm.jaql.lang.core.Context)
   */
  public JsonRecord eval(Context context) throws Exception
  {
    JsonRecord oldRec = (JsonRecord) exprs[0].eval(context);
    if (oldRec == null)
    {
      return null;
    }

    JsonRecord map = (JsonRecord) exprs[1].eval(context);
    if (map == null || map.size() == 0)
    {
      return oldRec;
    }

    
    BufferedJsonRecord newRec = new BufferedJsonRecord(oldRec.size()); // TODO: memory
    // TODO: create a JRecord that references another JRecord and overrides fields 
    for (Entry<JsonString, JsonValue> e : oldRec)
    {
      JsonString name = e.getKey();
      JsonString name2 = (JsonString) map.get(name);
      if (name2 != null)
      {
        name = name2;
      }
      newRec.add(name, e.getValue());
    }

    return newRec;
  }
}
