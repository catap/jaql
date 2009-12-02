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
package com.ibm.jaql.io.serialization.text.def;

import java.io.IOException;
import java.io.PrintStream;

import com.ibm.jaql.io.serialization.text.TextBasicSerializer;
import com.ibm.jaql.json.type.JsonRegex;
import com.ibm.jaql.json.type.JsonString;

public class RegexSerializer extends TextBasicSerializer<JsonRegex>
{
  TextBasicSerializer<JsonString> stringSerializer;

  public RegexSerializer(TextBasicSerializer<JsonString> stringSerializer)
  {
    this.stringSerializer = stringSerializer ;
  }
  
  @Override
  public void write(PrintStream out, JsonRegex value, int indent)
      throws IOException
  {
    //    out.print('/');
    //    if( regex.find("/") < 0 )
    //    {
    //      out.print(regex);      
    //    }
    //    else
    //    {
    //      for(int i = 0 ; i < regex.getLength() ; i++)
    //      {
    //        int c = regex.charAt(i);
    //        if( c == '/' )
    //        {
    //          out.print("\\/");
    //        }
    //        else
    //        {
    //          out.print((char)c);
    //        }
    //      }
    //    }
    //    out.print('/');
    
    out.print("regex(");
    
    stringSerializer.write(out, value.getInternalRegex(), indent);
    
    byte flags = value.getFlags();
    if (flags != 0)
    {
      out.print(",'");
      if ((flags & JsonRegex.GLOBAL) != 0) out.print('g');
      if ((flags & JsonRegex.MULTILINE) != 0) out.print('m');
      if ((flags & JsonRegex.CASE_INSENSITIVE) != 0) out.print('i');
      out.print('\'');
    }
    
    out.print(')');
  }
}
