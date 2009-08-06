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

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.net.protocols.Response;
import org.apache.nutch.protocol.Content;

import com.ibm.jaql.io.converter.ToJson;
import com.ibm.jaql.json.schema.OrSchema;
import com.ibm.jaql.json.schema.RecordSchema;
import com.ibm.jaql.json.schema.Schema;
import com.ibm.jaql.json.schema.SchemaFactory;
import com.ibm.jaql.json.type.BufferedJsonRecord;
import com.ibm.jaql.json.type.JsonBinary;
import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonType;
import com.ibm.jaql.json.type.JsonValue;

public class FromNutchContent extends HadoopRecordToJson<WritableComparable<?>, Writable> {

  public static enum Field {
    URL("url"),
    BASEURL("bUrl"),
    TYPE("type"),
    CONTENT("cont"),
    META("meta");
    
    public final String name;
    public final JsonString jsonName;
    
    Field(String n) {
      this.name = n;
      this.jsonName = new JsonString(name);
    }
  };
  
  private static final JsonString EMPTY_STRING = new JsonString();
  
  @Override
  protected ToJson<WritableComparable<?>> createKeyConverter() {
    // TODO Auto-generated method stub
    return null;
  }

  // TODO: the generic should be any subclass of Writable
  @Override
  protected ToJson<Writable> createValueConverter() {
    return new ToJson<Writable> () {
      
      private JsonString sVal = new JsonString();
      private JsonBinary bVal = new JsonBinary();
      
      public JsonValue convert(Writable src, JsonValue tgt)
      {
        if(! (src instanceof Content) ) {
          // FIXME: exception should be propagated up
          // just clear the fields
          BufferedJsonRecord r = null;
          try {
            r = (BufferedJsonRecord)tgt.getCopy(null);
            r.set(Field.URL.jsonName, EMPTY_STRING);
            r.set(Field.BASEURL.jsonName, EMPTY_STRING);
            r.set(Field.TYPE.jsonName, EMPTY_STRING);
            r.set(Field.CONTENT.jsonName, null);
            ((BufferedJsonRecord) r.get(Field.META.jsonName)).clear();
          } catch(Exception e) {
            throw new RuntimeException("error creating target record");
          }
          
          return r;
        }
        
        // expect src to be Content
        Content c = (Content)src;
        
        // expect tgt's value to be MemoryJRecord 
        BufferedJsonRecord r = (BufferedJsonRecord)tgt;
        
        // the metadata
        Metadata meta = c.getMetadata();
        
        // set the fixed fields
        ((JsonString)r.get(Field.URL.jsonName)).setCopy(c.getUrl().getBytes());
        ((JsonString)r.get(Field.BASEURL.jsonName)).setCopy(c.getBaseUrl().getBytes());
        ((JsonString)r.get(Field.TYPE.jsonName)).setCopy(c.getContentType().getBytes());
        
        String cTypeFromMeta = meta.get(Response.CONTENT_TYPE);
        String cTypeFromTop  = c.getContentType();
        JsonValue cValue = r.get(Field.CONTENT.jsonName);
        if( (cTypeFromMeta != null && cTypeFromMeta.indexOf("text") >= 0 ) ||
            (cTypeFromTop  != null && cTypeFromTop.indexOf("text") >= 0 ) ){
          sVal.setCopy(c.getContent());
          if(cValue.getEncoding().getType() != JsonType.STRING) 
            cValue = sVal;
        } else {
          bVal.setBytes(c.getContent());
          if(cValue.getEncoding().getType() != JsonType.BINARY)
            cValue = bVal;
        }
        
        // set the dynamic metadata
        BufferedJsonRecord mr = (BufferedJsonRecord)r.get(Field.META.jsonName);
        mr.clear();
        String[] names = meta.names();
        for(int i = 0; i < names.length; i++) {
          mr.add(new JsonString(names[i]), new JsonString(meta.get(names[i])));
        }
        
        return r;
      }
      
      public JsonValue createTarget()
      {
        int n = Field.values().length;
        BufferedJsonRecord r = new BufferedJsonRecord(n);
        r.add(Field.URL.jsonName, new JsonString());
        r.add(Field.BASEURL.jsonName, new JsonString());
        r.add(Field.TYPE.jsonName, new JsonString());
        r.add(Field.CONTENT.jsonName, sVal);
        r.add(Field.META.jsonName, new BufferedJsonRecord());
        
        return r;
      }
      
      public Schema getSchema()
      {
        return new RecordSchema(new RecordSchema.Field[] { 
            new RecordSchema.Field(Field.URL.jsonName, SchemaFactory.stringSchema(), false),
            new RecordSchema.Field(Field.BASEURL.jsonName, SchemaFactory.stringSchema(), false),
            new RecordSchema.Field(Field.TYPE.jsonName, SchemaFactory.stringSchema(), false),
            new RecordSchema.Field(Field.CONTENT.jsonName, 
                OrSchema.or(SchemaFactory.stringSchema(), SchemaFactory.binarySchema()), 
                false),
            new RecordSchema.Field(Field.META.jsonName, SchemaFactory.stringSchema(), false)
        }, SchemaFactory.stringSchema());
      }
    };
  }
  
}