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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import com.ibm.jaql.io.converter.ToJson;
import com.ibm.jaql.io.hadoop.converter.HadoopRecordToJson;
import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.type.BufferedJsonArray;
import com.ibm.jaql.json.type.BufferedJsonRecord;
import com.ibm.jaql.json.type.JsonArray;
import com.ibm.jaql.json.type.JsonRecord;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonValue;
import com.ibm.jaql.lang.util.JaqlUtil;

/**
 * This converter is used to convert a text-based data file, with an optional header, into
 * an array of JArrays or JRecords. Given a value assumed to be of type o.a.h.io.Text,
 * the value is first tokenized according to a delimiter and converted to either
 * a JArray or a JRecord. If no header is provided, then the input line is converted to an array.
 * Otherwise, the header is used to construct record field names. The header is assumed to be an
 * array of field names; it can be provided by reading a file of such names or as a literal array. 
 */
public class FromDelimitConverter extends HadoopRecordToJson<WritableComparable, Writable> {
  
  private static final Log LOG             = LogFactory.getLog(FromDelimitConverter.class.getName());
  
  public static final JsonString DELIMITER_NAME = new JsonString("delimiter");
  public static final JsonString HEADER_NAME = new JsonString("header");
  
  private String delimiter = ",";
  private JsonArray header = null;
  
  @Override
  public void init(JsonRecord options)
  {
    if(options != null) {
      // 1. check for delimiter override
      JsonValue arg = options.get(DELIMITER_NAME);
      if(arg != null) {
        delimiter = arg.toString();
      }
      
      // 2. check for header
      arg = options.get(HEADER_NAME);
      if(arg != null) {
        try {
          header = (JsonArray) arg;
        } catch(Exception e) {
          LOG.info("exception with header: " + arg + "," + e);
        }
      }
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.ibm.jaql.io.hadoop.converter.HadoopRecordToItem#createKeyConverter()
   */
  @Override
  protected ToJson<WritableComparable> createKeyConverter()
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
        
        String[] vals = new String(t.getBytes()).split(delimiter);
        try {
          if(header == null) {
            setArray(vals, (BufferedJsonArray)tgt);
          } else {
            setRecord(vals, header, (BufferedJsonRecord)tgt);
          }
          return tgt;
        } catch(Exception e) {
          throw new RuntimeException(e);
        }
      }
      
      private void setRecord(String[] vals, JsonArray names, BufferedJsonRecord tgt) throws Exception
      {
        int n = (int) names.count();
        if(n != vals.length) 
          throw new RuntimeException("values and header disagree in length: " + vals.length + "," + n);
        
        for(int i = 0; i < n; i++) {
          JsonString name = (JsonString) names.nth(i);
          ((JsonString)tgt.getRequired(name)).set(vals[i]);
        }
      }
      
      private void setArray(String[] vals, BufferedJsonArray tgt) {
        tgt.clear();
        int n = vals.length;
        for(int i = 0; i < n; i++) {
          tgt.add(new JsonString(vals[i])); // FIXME: memory
        }
      }
      
      public JsonValue createTarget()
      {
        if(header == null)
          return new BufferedJsonArray();
        else {
          int n = (int)header.count();
          BufferedJsonRecord r = new BufferedJsonRecord(n);
          try {
            for(int i = 0; i < n; i++) {
              r.add( JaqlUtil.enforceNonNull((JsonString)header.nth(i)), new JsonString());
            }
          } catch(Exception e) { throw new RuntimeException(e);}

          return r;
        }
      }
      
      public Schema getSchema()
      {
        if (header == null)
        {
          return SchemaFactory.arrayOrNullSchema();
        }
        else
        {
          return SchemaFactory.recordOrNullSchema();
        }
      }
    };
  }
}