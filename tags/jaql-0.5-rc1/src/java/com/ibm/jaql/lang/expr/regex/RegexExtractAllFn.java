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
package com.ibm.jaql.lang.expr.regex;

import java.util.regex.Matcher;

import com.ibm.jaql.json.schema.ArraySchema;
import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.type.BufferedJsonArray;
import com.ibm.jaql.json.type.JsonRegex;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.util.JsonIterator;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.IterExpr;
import com.ibm.jaql.lang.expr.function.DefaultBuiltInFunctionDescriptor;


/**
 * @jaqlDescription Capture all the substrings which match each group (A group is a pair of parentheses used to 
 * group subpatterns.) specified in the regular expression. Return a string array like 
 *  [[match1_group1, match1_group2 ...] , [match2_group1, match2_group2] ... ]
 * 
 * Usage:
 * [string] regex_extract(regex reg, string text)
 * 
 * regex_extract_all(regex("(a(b*))"),"abbabcd");
 * 
 * reg is the regular expression, text is the target string. For example, given a regular expression
 *   (a(b*))
 * it contains 3 groups:
 *   group 1: (a(b*)) 
 *   group 2: (b*) 
 * if input is "abbabcd", by use of regex_extract function, substrings matches each group(1-2) will be captured, this function
 * will return a string array, like
 *  [ 
 *   ["abb","bb"],
 *   ["ab","b"]
 *  ]
 *
 * where "abb" and "bb" is the first match of group 1 and 2 when scaning the text, "ab" and "b" is the second(last) match. 
 * 
 * @jaqlExample regex_extract_all(regex("(a(b*))+(c*)"),"abbabcd");
 * [ 
 *  [ "ab", "b", "c"]
 * ]
 * 
 * @jaqlExample regex_extract_all(regex("(a(b*))"),"abbabcd");
 *  [ 
 *   ["abb","bb"],
 *   ["ab","b"]
 *  ]
 * 
 */
public class RegexExtractAllFn extends IterExpr
{
  public final static Schema SCHEMA =   // [ [string?*]* ]?
    SchemaFactory.nullable( new ArraySchema(null, new ArraySchema(null, 
            SchemaFactory.nullable(SchemaFactory.stringSchema()))));
  
  public static class Descriptor extends DefaultBuiltInFunctionDescriptor.Par22
  {
    public Descriptor()
    {
      super("regex_extract_all", RegexExtractAllFn.class); // TODO: get a consistent naming scheme
    }
  }
  
  /**
   * @param args
   */
  public RegexExtractAllFn(Expr[] args)
  {
    super(args);
  }

  @Override
  public Schema getSchema()
  {
    return SCHEMA;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.lang.expr.core.IterExpr#iter(com.ibm.jaql.lang.core.Context)
   */
  public JsonIterator iter(final Context context) throws Exception
  {
    final JsonRegex regex = (JsonRegex) exprs[0].eval(context);
    if (regex == null)
    {
      return JsonIterator.NULL;
    }
    JsonString text = (JsonString) exprs[1].eval(context);
    if (text == null)
    {
      return JsonIterator.NULL;
    }
    final Matcher matcher = regex.takeMatcher();
    matcher.reset(text.toString());
    final int n = matcher.groupCount();    
    final BufferedJsonArray arr = new BufferedJsonArray(n); // TODO: memory
    
    return new JsonIterator(arr)
    {
      boolean atEnd = false;
      
      @Override
      public boolean moveNext() throws Exception
      {
        if( atEnd )
        {
          return false;
        }
        
        if( matcher.find() )
        {
          for(int i = 0 ; i < n ; i++)
          {
            String s = matcher.group(i+1);
            arr.set(i, s == null ? null : new JsonString(s)); // TODO: memory
          }
          return true;
        }
        
        regex.returnMatcher(matcher);
        atEnd = true;
        return false;
      }
    };
    
  }
}
