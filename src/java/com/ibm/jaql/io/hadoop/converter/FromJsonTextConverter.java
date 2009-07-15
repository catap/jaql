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
package com.ibm.jaql.io.hadoop.converter;

import java.io.StringReader;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import com.ibm.jaql.io.converter.ToJson;
import com.ibm.jaql.json.parser.JsonParser;
import com.ibm.jaql.json.parser.ParseException;
import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.type.JsonValue;

/**
 * 
 */
public class FromJsonTextConverter extends HadoopRecordToJson<WritableComparable<?>, Text>
{

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
  protected ToJson<Text> createValueConverter()
  {
    return new ToJson<Text>() {
      JsonParser parser = new JsonParser();
      
      public JsonValue convert(Text src, JsonValue tgt)
      {
        if (src == null) 
        {
          return null;
        }
        
        try
        {
          parser.ReInit(new StringReader(src.toString()));
          JsonValue value = parser.JsonVal();
          return value;
        }
        catch (ParseException pe)
        {
          throw new RuntimeException(pe);
        }
      }

      public JsonValue createTarget()
      {
        return null;
      }
      
      public Schema getSchema()
      {
        return SchemaFactory.anyOrNullSchema();
      }
    };
  }
}
