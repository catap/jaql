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

import com.ibm.jaql.json.type.JArray;
import com.ibm.jaql.json.type.JString;
import com.ibm.jaql.json.type.SpillJArray;

/**
 * 
 */
public class Split1
{
  private SpillJArray result    = new SpillJArray();
  private JString     resultStr = new JString();

  /**
   * @param jstr
   * @param jdelim
   * @return
   * @throws Exception
   */
  public JArray eval(JString jstr, JString jdelim) throws Exception
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
