package com.ibm.jaql.io.serialization.text.def;

import java.io.IOException;
import java.io.PrintStream;

import com.ibm.jaql.io.serialization.text.TextBasicSerializer;
import com.ibm.jaql.json.type.JsonRegex;
import com.ibm.jaql.json.type.JsonString;

public class JsonRegexSerializer extends TextBasicSerializer<JsonRegex>
{
  TextBasicSerializer<JsonString> stringSerializer;

  public JsonRegexSerializer(TextBasicSerializer<JsonString> stringSerializer)
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
