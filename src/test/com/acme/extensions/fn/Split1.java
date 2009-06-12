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
package com.acme.extensions.fn;

import com.ibm.jaql.json.type.JsonArray;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.SpilledJsonArray;

/**
 * 
 */
public class Split1
{
  private SpilledJsonArray result    = new SpilledJsonArray();
  private JsonString     resultStr = new JsonString();

  /**
   * @param jstr
   * @param jdelim
   * @return
   * @throws Exception
   */
  public JsonArray eval(JsonString jstr, JsonString jdelim) throws Exception
  {
    if (jstr == null || jdelim == null)
    {
      return null;
    }
    String str = jstr.toString();
    String delim = jdelim.toString();

    String[] splits = str.split(delim);

    result.clear();
    for (String s : splits)
    {
      resultStr.set(s);
      result.addCopy(resultStr);
    }

    return result;
  }
}
