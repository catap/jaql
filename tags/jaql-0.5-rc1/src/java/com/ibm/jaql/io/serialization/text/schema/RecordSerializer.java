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
package com.ibm.jaql.io.serialization.text.schema;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.Map.Entry;

import com.ibm.jaql.io.serialization.text.TextBasicSerializer;
import com.ibm.jaql.io.serialization.text.TextFullSerializer;
import com.ibm.jaql.io.serialization.text.def.StringSerializer;
import com.ibm.jaql.json.schema.RecordSchema;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.util.FastPrinter;

public class RecordSerializer extends TextBasicSerializer<JsonRecord>
{
  RecordSchema schema;
  int noRequiredOptional;
  TextBasicSerializer<JsonString> nameSerializer;
  FieldInfo[] allInfo;
  TextFullSerializer additionalSerializer;
  Set<JsonString> fieldNames;
  
  /** Stores information about a required or optional field. */
  private static class FieldInfo
  {
    RecordSchema.Field field;
    SchemaTextFullSerializer serializer;
    JsonString name;
    
    FieldInfo(RecordSchema.Field field, SchemaTextFullSerializer serializer)
    {
      this.field = field;
      this.serializer = serializer;
      this.name = (JsonString)field.getName().getCopy(null);
    }
  }
  
  public RecordSerializer(RecordSchema schema)
  {
    this.schema = schema;
    init();
    this.nameSerializer = new StringSerializer() ;
 }
  
  private void init()
  {
    // create data structures
    noRequiredOptional = schema.noRequiredOrOptional();
    allInfo = new FieldInfo[noRequiredOptional];
    fieldNames = new HashSet<JsonString>();
    
    // scan required and optional fields
    for (int pos=0; pos < noRequiredOptional; pos++)
    {
      RecordSchema.Field field = schema.getFieldByPosition(pos);
      FieldInfo k = new FieldInfo(field, new SchemaTextFullSerializer(field.getSchema()));
      fieldNames.add(field.getName());
      allInfo[pos] = k;
    }
    
    // scan additional fields
    if (schema.getAdditionalSchema() != null)
    {
      additionalSerializer = new SchemaTextFullSerializer(schema.getAdditionalSchema());      
    }
  }
  
  @Override
  public void write(FastPrinter out, JsonRecord value, int indent)
      throws IOException
  {
    out.print("{");
    
    indent += 2;
    String sep = "";
    
    // print required/optional fields
    int n=0;
    for (int i=0; i<noRequiredOptional; i++)
    {
      FieldInfo info = allInfo[i];
      JsonString name = info.name;
      if (value.containsKey(name))
      {
        out.println(sep);
        indent(out, indent);
        nameSerializer.write(out, name, indent);
        out.print(": ");
        info.serializer.write(out, value.get(name));
        sep = ",";
        n++;
      }
      else if (!info.field.isOptional())
      {
        throw new IllegalArgumentException("field missing: " + name);
      }
    }
    
    // print other fields
    if (n != value.size())
    {
      if (!schema.hasAdditional())
      {
        throw new IllegalArgumentException("too many fields");
      }
      Iterator<Entry<JsonString, JsonValue>> it = value.iteratorSorted();
      while (it.hasNext())
      {
        Entry<JsonString, JsonValue> e = it.next();
        if (fieldNames.contains(e.getKey())) continue;
        out.println(sep);
        indent(out, indent);
        nameSerializer.write(out, e.getKey(), indent);
        out.print(": ");
        additionalSerializer.write(out, e.getValue());
        sep = ",";
        n++;
      }
    }
    indent -= 2;
    
    if (sep.length() > 0) // if not empty record
    {
      out.println();
      indent(out, indent);
    }
    out.print("}");  
  }
  

}
