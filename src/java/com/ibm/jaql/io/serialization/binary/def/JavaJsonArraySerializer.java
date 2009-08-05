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

import com.ibm.jaql.io.serialization.binary.BinaryBasicSerializer;
import com.ibm.jaql.json.meta.MetaArray;
import com.ibm.jaql.json.type.JavaJsonArray;
import com.ibm.jaql.json.type.JsonValue;

class JavaJsonArraySerializer extends BinaryBasicSerializer<JavaJsonArray>
{

  @Override
  public JavaJsonArray newInstance()
  {
    return new JavaJsonArray();
  }

  
  @Override
  public JavaJsonArray read(DataInput in, JsonValue target) throws IOException
  {
    if (target == null || !(target instanceof JavaJsonArray)) {
      target = new JavaJsonArray();      
    }
    JavaJsonArray t = (JavaJsonArray)target;
    
    String className = in.readUTF(); // TODO: would like to compress out the class name...
    t.setClass(className);
    MetaArray metaArray = t.getMetaArray();
    Object value = metaArray.read(in, t.getValue());
    t.setValue(value);
    return t;
  }

  @Override
  public void write(DataOutput out, JavaJsonArray value) throws IOException
  {
    MetaArray metaArray = value.getMetaArray();
    out.writeUTF(metaArray.getClazz().getName()); // TODO: would like to compress out the class name...
    metaArray.write(out, value.getValue());
  }
  
  
  //TODO: efficient implementation of compare, skip, and copy
}
