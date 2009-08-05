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
package com.foobar.store;

import java.io.ByteArrayInputStream;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import com.ibm.jaql.io.converter.ToJson;
import com.ibm.jaql.io.hadoop.converter.HadoopRecordToJson;
import com.ibm.jaql.json.parser.JsonParser;
import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.type.JsonValue;

/**
 * 
 */
public class FromJSONSeqConverter extends HadoopRecordToJson<WritableComparable<?>,Writable>
{

  /**
   * @param w
   * @param i
   */
  private JsonValue convertWritableToItem(Writable w, JsonValue val)
  {
    if (w == null) return null;
    Text t = null;
    if (w instanceof Text)
    {
      t = (Text) w;
    }
    else
    {
      t = new Text(w.toString());
    }

    ByteArrayInputStream input = new ByteArrayInputStream(t.getBytes());
    JsonParser parser = new JsonParser(input);

    try
    {
      val = parser.JsonVal();
    }
    catch (Exception e)
    {
      throw new RuntimeException(e);
    }
    return val;
  }

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
    return new ToJson<Writable>()
    {

      public JsonValue convert(Writable src, JsonValue tgt)
      {
        return convertWritableToItem(src, tgt);
      }

      public JsonValue createTarget()
      {
        return null;
      }
      
      public Schema getSchema()
      {
        return SchemaFactory.anySchema();
      }
    };
  }

}
