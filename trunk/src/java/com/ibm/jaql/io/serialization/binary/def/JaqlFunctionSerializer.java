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
import java.lang.reflect.UndeclaredThrowableException;

import com.ibm.jaql.io.serialization.binary.BinaryBasicSerializer;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.core.JaqlFunction;

class JaqlFunctionSerializer extends BinaryBasicSerializer<JaqlFunction>
{
  @Override
  public JaqlFunction read(DataInput in, JsonValue target) throws IOException
  {
    JaqlFunction t;
    if (target==null || !(target instanceof JaqlFunction)) {
      t = new JaqlFunction();
    } else {
      t = (JaqlFunction)target;
    }
      
    String fnText = in.readUTF();
    try {
      t.set(fnText);
    } catch (Exception e) {
      throw new UndeclaredThrowableException(e);
    }
    return t;
  }

  @Override
  public void write(DataOutput out, JaqlFunction value) throws IOException
  {
    out.writeUTF(value.getText());
  }

}
