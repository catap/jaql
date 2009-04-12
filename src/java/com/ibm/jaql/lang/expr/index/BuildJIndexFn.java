/*
 * Copyright (C) IBM Corp. 2009.
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
package com.ibm.jaql.lang.expr.index;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;

import com.ibm.jaql.json.type.Item;
import com.ibm.jaql.json.type.JArray;
import com.ibm.jaql.json.type.JLong;
import com.ibm.jaql.json.type.JRecord;
import com.ibm.jaql.json.type.JString;
import com.ibm.jaql.json.util.Iter;
import com.ibm.jaql.lang.core.Context;
import com.ibm.jaql.lang.expr.core.Expr;
import com.ibm.jaql.lang.expr.core.JaqlFn;


@JaqlFn(fnName = "buildJIndex", minArgs = 2, maxArgs = 2)
public class BuildJIndexFn extends Expr
{
  private static final int indexSkip = 100;
  private static final int baseSkip = 1;
  private ArrayList<Index> indexes;
  private String loc;
  private JLong joffset = new JLong();
  
  public BuildJIndexFn(Expr[] exprs)
  {
    super(exprs);
  }

  @Override
  public Item eval(Context context) throws Exception
  {
    Item fdItem = exprs[1].eval(context);
    JRecord fd = (JRecord)fdItem.get();
    if( fd == null )
    {
      return Item.nil;
    }
    JString jloc = (JString)fd.getRequired("location").get();
    if( jloc == null )
    {
      return Item.nil;
    }
    loc = jloc.toString();
    
    int i = 0;
    File file = new File(loc+".idx"+i);
    while( file.exists() )
    {
      if( !file.delete() )
      {
        throw new IOException("couldn't delete index file: "+file);
      }
      i++;
      file = new File(loc+".idx"+i);
    }
    
    FileOutputStream baseFOS  = new FileOutputStream(loc+".base");
    DataOutputStream base = new DataOutputStream(new BufferedOutputStream(baseFOS));
    indexes = new ArrayList<Index>();
    indexes.add(new Index(loc, 0));
    indexes.add(new Index(loc, 1));
    Item[] kvpair = new Item[2];

    i=0;
    Item item;
    Iter iter = exprs[0].iter(context);
    while( (item = iter.next()) != null )
    {
      JArray arr = (JArray)item.get();
      arr.getTuple(kvpair);
      Item key = kvpair[0];
      Item val = kvpair[1];
      i++;
      if( i == baseSkip )
      {
        writeIndex(0,key,base.size());
        i = 0;
      }
      key.write(base);
      val.write(base);
    }
    
    for(Index index: indexes)
    {
      index.out.close();
      if( index.numKeys < indexSkip / 2 )
      {
        if( ! index.file.delete() )
        {
          throw new IOException("index couldn't be deleted: "+index.file);
        }
      }
    }
    
    return fdItem;
  }

  private void writeIndex(int level, Item key, long offset) throws IOException
  {
    Index index0 = indexes.get(level);
    Index index1 = indexes.get(level+1);
    index1.counter++;
    if( index1.counter == indexSkip )
    {
      if( indexes.size() == level + 2 )
      {
        indexes.add(new Index(loc,level+2));
      }
      writeIndex(level+1, key, index0.out.size());
    }
    key.write(index0.out);
    joffset.value = offset;
    joffset.write(index0.out);
    index0.numKeys++;
    index0.counter = 0;
  }

  static class Index
  {
    int counter;
    long numKeys;
    File file;
    FileOutputStream fos;
    DataOutputStream out;
    
    public Index(String loc, int level) throws IOException
    {
      file = new File(loc+".idx"+level);
      fos = new FileOutputStream(file);
      out = new DataOutputStream(new BufferedOutputStream(fos));
    }
  }
}
