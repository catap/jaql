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

import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.MutableJsonString;
import com.ibm.jaql.json.util.JsonIterator;

/**
 * 
 */
public class Split2
{
  /**
   * @param jstr
   * @param jdelim
   * @return
   * @throws Exception
   */
  public JsonIterator eval(JsonString jstr, JsonString jdelim) throws Exception
  {
    if (jstr == null || jdelim == null)
    {
      return null;
    }
    String str = jstr.toString();
    String delim = jdelim.toString();

    final String[] splits = str.split(delim);

    final MutableJsonString resultStr = new MutableJsonString();
    return new JsonIterator(resultStr) {
      int             i         = 0;
      
      public boolean moveNext()
      {
        if (i >= splits.length)
        {
          return false;
        }
        resultStr.setCopy(splits[i]);
        i++;
        return true; // currentValue == resultStr
      }
    };
  }
}
