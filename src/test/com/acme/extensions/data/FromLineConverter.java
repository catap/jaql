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
package com.acme.extensions.data;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import com.ibm.jaql.io.converter.ToJson;
import com.ibm.jaql.io.hadoop.converter.HadoopRecordToJson;
import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.json.type.MutableJsonString;

/**
 * Assumes that the "value" of a [key, value] pair is of type
 * o.a.h.io.Text. For each such value, this converter returns a JsonString.
 */
public class FromLineConverter extends HadoopRecordToJson<WritableComparable<?>, Writable> {
  
  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.io.hadoop.converter.HadoopRecordToItem#createKeyConverter()
   */
  @Override
  protected ToJson<WritableComparable<?>> createKeyConverter()
  {
    return null;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.io.hadoop.converter.HadoopRecordToItem#createValConverter()
   */
  @Override
  protected ToJson<Writable> createValueConverter()
  {
    return new ToJson<Writable>() {
      public JsonValue convert(Writable src, JsonValue tgt)
      {
        if (src == null) return null;
        Text t = null;
        if (src instanceof Text)
        {
          t = (Text) src;
        }
        else
        {
          throw new RuntimeException("tried to convert from: " + src);
        }
        ((MutableJsonString)tgt).set(t.getBytes(), t.getLength());
        return tgt;
      }
      
      public JsonValue createTarget()
      {
        return new MutableJsonString();
      }
      
      public Schema getSchema()
      {
        return SchemaFactory.stringOrNullSchema();
      }
    };
  }
}