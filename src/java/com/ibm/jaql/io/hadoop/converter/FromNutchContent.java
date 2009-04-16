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

import com.ibm.jaql.io.converter.FromItem;
import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.json.type.JBinary;
import com.ibm.jaql.json.type.JString;
import com.ibm.jaql.json.type.MemoryJRecord;

public class FromNutchContent extends HadoopRecordToItem<WritableComparable, Writable> {

  public static enum Field {
    URL("url"),
    BASEURL("bUrl"),
    TYPE("type"),
    CONTENT("cont"),
    META("meta");
    
    public final String name;
    
    Field(String n) {
      this.name = n;
    }
  };
  
  @Override
  protected FromItem<WritableComparable> createKeyConverter() {
    // TODO Auto-generated method stub
    return null;
  }

  // TODO: the generic should be any subclass of Writable
  @Override
  protected FromItem<Writable> createValConverter() {
    return new FromItem<Writable> () {
      
      private JString sVal = new JString();
      private JBinary bVal = new JBinary();
      
      public void convert(Writable src, Item tgt)
      {
        // expect src to be Content
        Content c = (Content)src;
        
        // expect tgt's value to be MemoryJRecord 
        MemoryJRecord r = (MemoryJRecord)tgt.get();
        
        // the metadata
        Metadata meta = c.getMetadata();
        
        // set the fixed fields
        ((JString)r.getValue(Field.URL.name).get()).copy(c.getUrl().getBytes());
        ((JString)r.getValue(Field.BASEURL.name).get()).copy(c.getBaseUrl().getBytes());
        ((JString)r.getValue(Field.TYPE.name).get()).copy(c.getContentType().getBytes());
        
        String cType = meta.get(Response.CONTENT_TYPE);
        Item cItem = r.getValue(Field.CONTENT.name);
        if(cType != null && cType.indexOf("text") >= 0) {
          sVal.copy(c.getContent());
          if(cItem.getType() == Item.Type.STRING) 
            cItem.set(sVal);
        } else {
          bVal.setBytes(c.getContent());
          if(cItem.getType() == Item.Type.BINARY)
            cItem.set(bVal);
        }
        
        // set the dynamic metadata
        MemoryJRecord mr = (MemoryJRecord)r.getValue(Field.META.name).get();
        mr.clear();
        String[] names = meta.names();
        for(int i = 0; i < names.length; i++) {
          mr.add(names[i], new JString(meta.get(names[i])));
        }
      }
      
      public Item createTarget()
      {
        int n = Field.values().length;
        MemoryJRecord r = new MemoryJRecord(n);
        r.add(Field.URL.name, new JString());
        r.add(Field.BASEURL.name, new JString());
        r.add(Field.TYPE.name, new JString());
        r.add(Field.CONTENT.name, sVal);
        r.add(Field.META.name, new MemoryJRecord());
        
        return new Item(r);
      }
    };
  }
  
}