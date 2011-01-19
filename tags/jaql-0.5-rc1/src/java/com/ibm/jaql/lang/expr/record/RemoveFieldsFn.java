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

import java.util.HashSet;
import java.util.Map.Entry;

import com.ibm.jaql.json.type.BufferedJsonRecord;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;

/**
 * @jaqlDescription Remove fields of a record by keys.
 * 
 * Usage:
 * record removeFields(record target, array names);
 * 
 * names is an array with one or more string key names, removeFields will remove fields in target record if its key appears in names.
 * 
 * @jaqlExample removeFields({a:1,b:2},["a"]);
 * {
 *  "b":2
 * }
 * 
 */
public class RemoveFieldsFn extends Expr
{
  public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par22
  {
    public Descriptor()
    {
      super("removeFields", RemoveFieldsFn.class);
    }
  }
  
  /**
   * removeFields(rec, [names])
   * 
   * @param exprs
   */
  public RemoveFieldsFn(Expr[] exprs)
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
    JsonRecord rec = (JsonRecord) exprs[0].eval(context);
    if (rec == null)
    {
      return null;
    }

    JsonIterator iter = exprs[1].iter(context);
    // TODO: it would be great to detect a constant list of names here
    HashSet<JsonString> removeNames = new HashSet<JsonString>(); // TODO: memory
    for (JsonValue name : iter)
    {
      JsonString nm = (JsonString) name;
      removeNames.add(new JsonString(nm)); // TODO: memory
    }

    BufferedJsonRecord outRec = new BufferedJsonRecord(); // TODO: memory

    for (Entry<JsonString, JsonValue> e : rec)
    {
      JsonString nm = e.getKey();
      if (!removeNames.contains(nm))
      {
        outRec.add(nm, e.getValue());
      }
    }

    return outRec;
  }
}
