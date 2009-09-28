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
package com.ibm.jaql.io.serialization.binary.def;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.StringReader;

import com.ibm.jaql.io.serialization.binary.BinaryBasicSerializer;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.function.Function;
import com.ibm.jaql.lang.parser.JaqlLexer;
import com.ibm.jaql.lang.parser.JaqlParser;

class FunctionSerializer extends BinaryBasicSerializer<Function>
{
  @Override
  public Function read(DataInput in, JsonValue target) throws IOException
  {
    String fnText = in.readUTF();
    JaqlLexer lexer = new JaqlLexer(new StringReader(fnText));
    JaqlParser parser = new JaqlParser(lexer);
    try
    {
      Expr fe = parser.stmt();
      if (!fe.isCompileTimeComputable().always())
      {
        throw new IOException("input value is not a function literal");
      }
      return (Function)fe.eval(null);
      
    } catch (Exception e)
    {
      throw new IOException(e);
    }
  }

  @Override
  public void write(DataOutput out, Function value) throws IOException
  {
    out.writeUTF(value.getText());
  }

}
