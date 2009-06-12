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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.util.JsonIterator;

/**
 * 
 */
public class Grep
{
  /**
   * @param regex
   * @param jstrs
   * @return
   * @throws Exception
   */
  public JsonIterator eval(JsonString regex, JsonIterator jstrs) throws Exception
  {
    return eval(regex, null, jstrs);
  }

  /**
   * @param regex
   * @param flags
   * @param jstrs
   * @return
   * @throws Exception
   */
  public JsonIterator eval(JsonString regex, JsonString flags, final JsonIterator jstrs)
      throws Exception
  {
    if (regex == null || jstrs == null)
    {
      return null;
    }

    int f = 0;
    boolean global1 = false;
    if (flags != null)
    {
      String s = flags.toString();
      for (int i = 0; i < flags.getLength(); i++)
      {
        switch (s.charAt(i))
        {
          case 'g' :
            global1 = true;
            break;
          case 'm' :
            f |= Pattern.MULTILINE;
            break;
          case 'i' :
            f |= Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CASE;
            break;
          default :
            throw new IllegalArgumentException("unknown regex flag: "
                + (char) s.charAt(i));
        }
      }
    }
    Pattern pattern = Pattern.compile(regex.toString(), f);

    final Matcher matcher = pattern.matcher("");
    final boolean global = global1;

    final JsonString resultStr = new JsonString();

    return new JsonIterator(resultStr) {
      private boolean needInput = true;

      public boolean moveNext() throws Exception
      {
        while (true)
        {
          if (needInput)
          {
            if (!jstrs.moveNextNonNull())
            {
              return false;
            }
            JsonString jstr = (JsonString) jstrs.current(); // could raise a cast error
            matcher.reset(jstr.toString());
          }
          if (matcher.find())
          {
            resultStr.set(matcher.group());
            needInput = !global;
            return true; // currentValue == resultStr
          }
          needInput = true;
        }
      }
    };
  }
}
